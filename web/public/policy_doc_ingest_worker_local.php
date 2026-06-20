#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_DOC_EMBED_MODEL = 'text-embedding-3-small';
const POLICY_DOC_EMBED_BATCH_SIZE = 32;
const POLICY_DOC_MAX_CHARS_PER_CHUNK = 8000;
const POLICY_DOC_TEXT_DIR = '/tmp/policy-document-text';

function parseCliArgs(array $argv): array
{
    $args = [];
    foreach (array_slice($argv, 1) as $a) {
        if (preg_match('~^--([a-z0-9_-]+)=(.*)~i', $a, $m)) {
            $args[strtolower($m[1])] = $m[2];
        }
    }

    return [
        'job_id' => isset($args['job']) ? (int) $args['job'] : null,
        'raw' => $args,
    ];
}

function policy_pg_pdo(): PDO
{
    static $pg = null;
    if ($pg instanceof PDO) {
        return $pg;
    }

    $host = (string) docling_env('PGHOST', docling_env('PG_HOST', '127.0.0.1'));
    $port = (string) docling_env('PGPORT', docling_env('PG_PORT', '5432'));
    $db = (string) docling_env('PGDATABASE', docling_env('PG_DB', 'docs_db'));
    $user = (string) docling_env('PGUSER', docling_env('PG_USER', 'webapp'));
    $pass = (string) docling_env('PGPASSWORD', docling_env('PG_PASSWORD', ''));
    $sslmode = (string) docling_env('PGSSLMODE', docling_env('PG_SSLMODE', 'require'));

    $dsn = sprintf('pgsql:host=%s;port=%s;dbname=%s;sslmode=%s', $host, $port, $db, $sslmode);
    $pg = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ]);
    return $pg;
}

function job_log_local(int $jobId, string $message): void
{
    error_log(sprintf('[policy-save:%d] %s', $jobId, $message));
}

function job_update_local(int $jobId, array $fields): void
{
    docling_job_update($jobId, $fields);
}

function job_finish_local(int $jobId, array $payload, string $status = 'completed'): void
{
    docling_job_finish($jobId, $payload, $status);
}

function policy_worker_fail(int $jobId, string $message, array $extra = [], int $code = 1): never
{
    $payload = ['success' => false, 'error' => $message] + $extra;
    docling_job_fail($jobId, $message, $payload);
    job_log_local($jobId, 'ERROR ' . $message);
    exit($code);
}

function policy_worker_require_file(string $path, string $message): void
{
    if ($path === '' || !is_file($path) || !is_readable($path)) {
        throw new RuntimeException($message);
    }
}

function policy_tokenize_count(string $text): int
{
    $words = preg_split('/\s+/u', trim($text)) ?: [];
    $words = array_values(array_filter($words, static fn($w) => $w !== ''));
    return count($words);
}

function policy_short_summary(string $text, int $n = 20): string
{
    $words = preg_split('/\s+/u', trim($text)) ?: [];
    $words = array_values(array_filter($words, static fn($w) => $w !== ''));
    return implode(' ', array_slice($words, 0, $n));
}

function policy_vector_literal(array $vector): string
{
    return '[' . implode(',', array_map(
        static fn($x) => rtrim(rtrim(sprintf('%.10F', (float) $x), '0'), '.'),
        $vector
    )) . ']';
}

function policy_mean_vector(array $vectors): ?array
{
    if ($vectors === []) {
        return null;
    }
    $first = reset($vectors);
    if (!is_array($first) || $first === []) {
        return null;
    }
    $dim = count($first);
    $sum = array_fill(0, $dim, 0.0);
    $count = 0;
    foreach ($vectors as $vector) {
        if (!is_array($vector) || count($vector) !== $dim) {
            continue;
        }
        foreach ($vector as $i => $value) {
            $sum[$i] += (float) $value;
        }
        $count++;
    }
    if ($count === 0) {
        return null;
    }
    return array_map(static fn($v) => $v / $count, $sum);
}

function policy_embed_batch(array $inputs, string $model): array
{
    $apiKey = (string) docling_env('OPENAI_API_KEY', '');
    if ($apiKey === '') {
        throw new RuntimeException('OPENAI_API_KEY is not configured.');
    }
    $payload = [
        'model' => $model,
        'input' => array_map(
            static fn($x) => mb_substr((string) $x, 0, POLICY_DOC_MAX_CHARS_PER_CHUNK),
            $inputs
        ),
    ];
    $ch = curl_init('https://api.openai.com/v1/embeddings');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Authorization: Bearer ' . $apiKey,
        ],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT => 180,
    ]);
    $raw = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    curl_close($ch);

    if ($raw === false) {
        throw new RuntimeException('Embedding request failed: ' . $err);
    }
    $json = json_decode((string) $raw, true);
    if (!is_array($json) || $status >= 400 || isset($json['error'])) {
        throw new RuntimeException('Embedding API error.');
    }
    $rows = $json['data'] ?? [];
    $out = [];
    foreach ($rows as $row) {
        $embedding = $row['embedding'] ?? null;
        $out[] = is_array($embedding) ? array_map('floatval', $embedding) : null;
    }
    return $out;
}

function policy_parse_page_chunks(string $text): array
{
    $raw = trim($text);
    if ($raw === '') {
        return [];
    }

    $parts = preg_split('/===== PAGE\s+(\d+)\s+=====/u', $raw, -1, PREG_SPLIT_DELIM_CAPTURE) ?: [];
    $chunks = [];
    if (count($parts) >= 3) {
        for ($i = 1; $i < count($parts); $i += 2) {
            $pageNo = (int) ($parts[$i] ?? 0);
            $pageText = trim((string) ($parts[$i + 1] ?? ''));
            if ($pageNo > 0 && $pageText !== '') {
                $chunks[] = [
                    'page' => $pageNo,
                    'text' => $pageText,
                ];
            }
        }
    }

    if ($chunks === [] && $raw !== '') {
        $chunks[] = [
            'page' => 1,
            'text' => $raw,
        ];
    }

    return $chunks;
}

function policy_sanitize_stem(string $value): string
{
    $value = strtolower(trim($value));
    $value = preg_replace('/[^a-z0-9]+/i', '_', $value) ?? $value;
    $value = trim($value, '_');
    return $value !== '' ? $value : 'policy_document';
}

function policy_write_text_file(string $title, string $text, string $sha256): string
{
    if (!is_dir(POLICY_DOC_TEXT_DIR)) {
        @mkdir(POLICY_DOC_TEXT_DIR, 0775, true);
    }
    $stem = policy_sanitize_stem($title);
    $path = sprintf('%s/%s_%s_%s.txt', rtrim(POLICY_DOC_TEXT_DIR, '/'), date('Ymd_His'), $stem, substr($sha256, 0, 10));
    if (@file_put_contents($path, $text) === false) {
        throw new RuntimeException('Failed to write policy text file.');
    }
    return $path;
}

function policy_upsert_document(PDO $pg, array $docRow): string
{
    $sql = "
      INSERT INTO public.documents (
        source_file, sha256, bytes, mime_type, pages, title,
        application_ref, document_type, local_authority, originator,
        document_date, meta, provenance, full_text, doc_vec, token_count,
        lpa_code, original_filename, site_point
      ) VALUES (
        :source_file, :sha256, :bytes, :mime_type, :pages, :title,
        :application_ref, :document_type, :local_authority, :originator,
        :document_date, CAST(:meta AS jsonb), CAST(:provenance AS jsonb), :full_text, CAST(:doc_vec AS vector), :token_count,
        :lpa_code, :original_filename, NULL
      )
      ON CONFLICT (sha256) WHERE sha256 IS NOT NULL
      DO UPDATE SET
        source_file = EXCLUDED.source_file,
        bytes = EXCLUDED.bytes,
        mime_type = EXCLUDED.mime_type,
        pages = COALESCE(EXCLUDED.pages, public.documents.pages),
        title = COALESCE(EXCLUDED.title, public.documents.title),
        application_ref = COALESCE(EXCLUDED.application_ref, public.documents.application_ref),
        document_type = COALESCE(EXCLUDED.document_type, public.documents.document_type),
        local_authority = COALESCE(EXCLUDED.local_authority, public.documents.local_authority),
        originator = COALESCE(EXCLUDED.originator, public.documents.originator),
        document_date = COALESCE(EXCLUDED.document_date, public.documents.document_date),
        meta = EXCLUDED.meta,
        provenance = EXCLUDED.provenance,
        full_text = EXCLUDED.full_text,
        doc_vec = COALESCE(EXCLUDED.doc_vec, public.documents.doc_vec),
        token_count = EXCLUDED.token_count,
        lpa_code = COALESCE(EXCLUDED.lpa_code, public.documents.lpa_code),
        original_filename = COALESCE(EXCLUDED.original_filename, public.documents.original_filename),
        updated_at = now()
      RETURNING id::text
    ";
    $st = $pg->prepare($sql);
    $st->execute([
        ':source_file' => $docRow['source_file'],
        ':sha256' => $docRow['sha256'],
        ':bytes' => $docRow['bytes'],
        ':mime_type' => $docRow['mime_type'],
        ':pages' => $docRow['pages'],
        ':title' => $docRow['title'],
        ':application_ref' => $docRow['application_ref'],
        ':document_type' => $docRow['document_type'],
        ':local_authority' => $docRow['local_authority'],
        ':originator' => $docRow['originator'],
        ':document_date' => $docRow['document_date'],
        ':meta' => json_encode($docRow['meta'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        ':provenance' => json_encode($docRow['provenance'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        ':full_text' => $docRow['full_text'],
        ':doc_vec' => $docRow['doc_vec'] ? policy_vector_literal($docRow['doc_vec']) : null,
        ':token_count' => $docRow['token_count'],
        ':lpa_code' => $docRow['lpa_code'],
        ':original_filename' => $docRow['original_filename'],
    ]);
    $id = $st->fetchColumn();
    if (!is_string($id) || $id === '') {
        throw new RuntimeException('Failed to upsert public.documents row.');
    }
    return $id;
}

function policy_upsert_chunk(PDO $pg, array $chunkRow): void
{
    $sql = "
      INSERT INTO public.chunks
        (doc_id, kind, page, position_json, summary, text, embedding, natural_key)
      VALUES
        (CAST(:doc_id AS uuid), :kind, :page, CAST(:position_json AS jsonb), :summary, :text, CAST(:embedding AS vector), :natural_key)
      ON CONFLICT (doc_id, natural_key)
      DO UPDATE SET
        kind = EXCLUDED.kind,
        page = EXCLUDED.page,
        position_json = EXCLUDED.position_json,
        summary = EXCLUDED.summary,
        text = EXCLUDED.text,
        embedding = COALESCE(EXCLUDED.embedding, public.chunks.embedding)
    ";
    $st = $pg->prepare($sql);
    $st->execute([
        ':doc_id' => $chunkRow['doc_id'],
        ':kind' => $chunkRow['kind'],
        ':page' => $chunkRow['page'],
        ':position_json' => json_encode($chunkRow['position_json'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        ':summary' => $chunkRow['summary'],
        ':text' => $chunkRow['text'],
        ':embedding' => $chunkRow['embedding'] ? policy_vector_literal($chunkRow['embedding']) : null,
        ':natural_key' => $chunkRow['natural_key'],
    ]);
}

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Wrong SAPI\n");
    exit(2);
}

$cli = parseCliArgs($argv);
$jobId = (int) ($cli['job_id'] ?? 0);
$raw = $cli['raw'] ?? [];
$requestJsonPath = (string) ($raw['request_json_path'] ?? '');

if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job\n");
    exit(2);
}

try {
    policy_worker_require_file($requestJsonPath, 'Policy-document request JSON file is missing.');
} catch (Throwable $e) {
    policy_worker_fail($jobId, $e->getMessage(), [], 2);
}

$requestRaw = file_get_contents($requestJsonPath);
$request = json_decode((string) $requestRaw, true);
if (!is_array($request)) {
    policy_worker_fail($jobId, 'Policy-document request JSON is invalid.', [], 2);
}

job_update_local($jobId, [
    'status' => 'running',
    'kind' => 'policy/save_document',
    'percentage_complete' => 5,
    'current_action' => 'preparing policy document ingestion',
]);

try {
    $pg = policy_pg_pdo();
    $title = trim((string) ($request['title'] ?? ''));
    $organisation = trim((string) ($request['organisation'] ?? ''));
    $text = trim((string) ($request['extracted_text'] ?? ''));
    $documentDate = trim((string) ($request['document_date'] ?? ''));
    $documentDate = preg_match('/^\d{4}-\d{2}-\d{2}$/', $documentDate) ? $documentDate : null;
    $originalFilename = trim((string) ($request['original_filename'] ?? ''));
    $sourceMode = trim((string) ($request['source_mode'] ?? 'upload'));
    $sourceUrl = trim((string) ($request['source_url'] ?? ''));
    $doclingJobId = (int) ($request['docling_job_id'] ?? 0);
    $doclingPageCount = (int) ($request['docling_page_count'] ?? 0);
    $doclingSourceSha256 = trim((string) ($request['docling_source_sha256'] ?? ''));

    if ($title === '' || $organisation === '' || $text === '') {
        policy_worker_fail($jobId, 'Missing required policy-document fields.', [], 2);
    }

    $pageChunks = policy_parse_page_chunks($text);
    if ($pageChunks === []) {
        policy_worker_fail($jobId, 'No non-empty page chunks could be derived from extracted text.');
    }

    job_update_local($jobId, [
        'percentage_complete' => 20,
        'current_action' => 'embedding policy pages',
    ]);

    $chunkEmbeddings = [];
    $embeddingWarnings = [];
    $validVectors = [];
    for ($offset = 0; $offset < count($pageChunks); $offset += POLICY_DOC_EMBED_BATCH_SIZE) {
        $batch = array_slice($pageChunks, $offset, POLICY_DOC_EMBED_BATCH_SIZE);
        $texts = array_map(static fn($row) => $row['text'], $batch);
        try {
            $embedded = policy_embed_batch($texts, POLICY_DOC_EMBED_MODEL);
        } catch (Throwable $e) {
            foreach ($batch as $row) {
                $chunkEmbeddings[$row['page']] = null;
                $embeddingWarnings[] = ['page' => $row['page'], 'error' => $e->getMessage()];
            }
            continue;
        }
        foreach ($batch as $i => $row) {
            $embedding = $embedded[$i] ?? null;
            $chunkEmbeddings[$row['page']] = $embedding;
            if (is_array($embedding)) {
                $validVectors[] = $embedding;
            } else {
                $embeddingWarnings[] = ['page' => $row['page'], 'error' => 'Embedding missing for page'];
            }
        }
    }

    $docVec = policy_mean_vector($validVectors);
    if ($docVec === null) {
        policy_worker_fail($jobId, 'All page embeddings failed.', ['embedding_warnings' => $embeddingWarnings]);
    }

    job_update_local($jobId, [
        'percentage_complete' => 55,
        'current_action' => 'writing policy text file',
    ]);

    $contentSha = hash('sha256', $text);
    $shaSeed = $sourceMode === 'url' && $sourceUrl !== '' ? ($contentSha . '|' . $sourceUrl) : $contentSha;
    $sha256 = hash('sha256', $shaSeed);
    $sourceFile = policy_write_text_file($title, $text, $sha256);
    $bytes = filesize($sourceFile) ?: strlen($text);

    job_update_local($jobId, [
        'percentage_complete' => 70,
        'current_action' => 'upserting policy document',
    ]);

    $meta = [
        'pipeline' => 'policy_document_upload',
        'source_mode' => $sourceMode,
        'source_url' => $sourceUrl !== '' ? $sourceUrl : null,
        'original_filename' => $originalFilename !== '' ? $originalFilename : null,
        'docling_job_id' => $doclingJobId > 0 ? $doclingJobId : null,
        'docling_page_count' => $doclingPageCount > 0 ? $doclingPageCount : count($pageChunks),
        'docling_source_sha256' => $doclingSourceSha256 !== '' ? $doclingSourceSha256 : null,
        'classification_model' => (string) ($request['classification_model'] ?? 'gpt-4o-mini'),
        'page_chunk_count' => count($pageChunks),
        'embedding_model' => POLICY_DOC_EMBED_MODEL,
        'embedding_warnings' => $embeddingWarnings,
    ];
    $provenance = [[
        'via' => 'hetzner_docling_pipeline',
        'source_mode' => $sourceMode,
        'source_url' => $sourceUrl !== '' ? $sourceUrl : null,
        'docling_job_id' => $doclingJobId > 0 ? $doclingJobId : null,
    ]];
    $docId = policy_upsert_document($pg, [
        'source_file' => $sourceFile,
        'sha256' => $sha256,
        'bytes' => $bytes,
        'mime_type' => 'text/plain',
        'pages' => $doclingPageCount > 0 ? $doclingPageCount : count($pageChunks),
        'title' => $title,
        'application_ref' => null,
        'document_type' => 'policy_document',
        'local_authority' => null,
        'originator' => $organisation,
        'document_date' => $documentDate,
        'meta' => $meta,
        'provenance' => $provenance,
        'full_text' => $text,
        'doc_vec' => $docVec,
        'token_count' => policy_tokenize_count($text),
        'lpa_code' => null,
        'original_filename' => $originalFilename !== '' ? $originalFilename : ($sourceUrl !== '' ? basename(parse_url($sourceUrl, PHP_URL_PATH) ?: 'policy_document.pdf') : 'policy_document.pdf'),
    ]);

    job_update_local($jobId, [
        'percentage_complete' => 85,
        'current_action' => 'upserting page chunks',
    ]);

    $chunksInserted = 0;
    foreach ($pageChunks as $chunk) {
        $embedding = $chunkEmbeddings[$chunk['page']] ?? null;
        if (!is_array($embedding)) {
            continue;
        }
        policy_upsert_chunk($pg, [
            'doc_id' => $docId,
            'kind' => 'page',
            'page' => $chunk['page'],
            'position_json' => ['page' => $chunk['page'], 'source' => 'docling_pagewise'],
            'summary' => policy_short_summary($chunk['text']),
            'text' => $chunk['text'],
            'embedding' => $embedding,
            'natural_key' => 'policy_document:page:' . $chunk['page'],
        ]);
        $chunksInserted++;
    }

    @unlink($requestJsonPath);

    $payload = [
        'success' => true,
        'doc_id' => $docId,
        'source_file' => $sourceFile,
        'sha256' => $sha256,
        'title' => $title,
        'organisation' => $organisation,
        'document_date' => $documentDate,
        'document_type' => 'policy_document',
        'pages' => count($pageChunks),
        'chunks_inserted' => $chunksInserted,
        'embedding_model' => POLICY_DOC_EMBED_MODEL,
        'docling_job_id' => $doclingJobId > 0 ? $doclingJobId : null,
        'source_mode' => $sourceMode,
        'source_url' => $sourceUrl !== '' ? $sourceUrl : null,
        'embedding_warnings' => $embeddingWarnings,
    ];

    job_finish_local($jobId, $payload, 'completed');
    job_log_local($jobId, 'Policy document ingestion completed.');
    exit(0);
} catch (Throwable $e) {
    policy_worker_fail($jobId, $e->getMessage(), [], 1);
}
