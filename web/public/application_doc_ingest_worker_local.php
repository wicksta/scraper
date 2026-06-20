#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const APPLICATION_DOC_UPLOAD_DIR = '/var/tmp/ngist_application_docs';
const APPLICATION_DOC_SOURCE_SUBDIR = 'otso_application_docs';
const APPLICATION_DOC_PROVENANCE_TYPE = 'guided_letter_upload';
const APPLICATION_DOC_CLASSIFIER_MODEL = 'gpt-4o-mini';

function application_doc_parse_cli(array $argv): array
{
    $args = [];
    foreach (array_slice($argv, 1) as $arg) {
        if (preg_match('~^--([a-z0-9_-]+)=(.*)~i', $arg, $m)) {
            $args[strtolower($m[1])] = $m[2];
        }
    }

    return [
        'job_id' => isset($args['job']) ? (int) $args['job'] : null,
        'raw' => $args,
    ];
}

function application_doc_pg_pdo(): PDO
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

function application_doc_fail(int $jobId, string $message, array $extra = [], int $code = 1): never
{
    $payload = ['success' => false, 'error' => $message] + $extra;
    docling_job_fail($jobId, $message, $payload);
    error_log(sprintf('[application-doc-save:%d] ERROR %s', $jobId, $message));
    exit($code);
}

function application_doc_require_file(string $path, string $message): void
{
    if ($path === '' || !is_file($path) || !is_readable($path)) {
        throw new RuntimeException($message);
    }
}

function application_doc_token_count(string $text): int
{
    $words = preg_split('/\s+/u', trim($text)) ?: [];
    $words = array_values(array_filter($words, static fn($w) => $w !== ''));
    return count($words);
}

function application_doc_safe_filename(string $name): string
{
    $name = preg_replace('/[^A-Za-z0-9._-]+/', '_', trim($name)) ?? 'upload.pdf';
    $name = trim($name, '._-');
    return $name !== '' ? $name : 'upload.pdf';
}

function application_doc_strip_fences(string $text): string
{
    $trimmed = trim($text);
    $trimmed = preg_replace('/^```(?:json)?\s*/i', '', $trimmed) ?? $trimmed;
    $trimmed = preg_replace('/\s*```$/', '', $trimmed) ?? $trimmed;
    return trim($trimmed);
}

function application_doc_classify(string $snippet, string $userTitle = '', string $userRole = ''): array
{
    $apiKey = (string) docling_env('OPENAI_API_KEY', '');
    if ($apiKey === '') {
        return ['ok' => false, 'error' => 'OPENAI_API_KEY is not configured.'];
    }

    $payload = [
        'model' => APPLICATION_DOC_CLASSIFIER_MODEL,
        'temperature' => 0,
        'response_format' => ['type' => 'json_object'],
        'messages' => [
            [
                'role' => 'system',
                'content' => 'You identify planning application documents from extracted text. Return only valid JSON.',
            ],
            [
                'role' => 'user',
                'content' => implode("\n", [
                    'A user has uploaded a planning application document and already supplied a title/role where known.',
                    'Use the first 10,000 characters to make a best-effort identification.',
                    'Return strict JSON with keys:',
                    '- title_hint',
                    '- document_kind',
                    '- originator',
                    '- summary',
                    'Keep each field short. Do not invent detail if unclear.',
                    '',
                    'USER_TITLE: ' . ($userTitle !== '' ? $userTitle : 'n/a'),
                    'USER_ROLE: ' . ($userRole !== '' ? $userRole : 'n/a'),
                    '',
                    'TEXT:',
                    mb_substr($snippet, 0, 10000),
                ]),
            ],
        ],
    ];

    $ch = curl_init('https://api.openai.com/v1/chat/completions');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Authorization: Bearer ' . $apiKey,
        ],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT => 120,
    ]);

    $raw = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    curl_close($ch);

    if ($raw === false) {
        return ['ok' => false, 'error' => 'OpenAI request failed.', 'details' => $err];
    }

    $json = json_decode((string) $raw, true);
    if (!is_array($json)) {
        return ['ok' => false, 'error' => 'OpenAI returned invalid JSON.'];
    }
    if ($status >= 400 || isset($json['error'])) {
        return ['ok' => false, 'error' => 'OpenAI returned an error.', 'details' => $json];
    }

    $content = (string) ($json['choices'][0]['message']['content'] ?? '');
    $decoded = json_decode(application_doc_strip_fences($content), true);
    if (!is_array($decoded)) {
        return ['ok' => false, 'error' => 'Assistant output was not valid JSON.', 'assistant_raw' => $content];
    }

    return [
        'ok' => true,
        'classification' => [
            'title_hint' => trim((string) ($decoded['title_hint'] ?? '')) ?: null,
            'document_kind' => trim((string) ($decoded['document_kind'] ?? '')) ?: null,
            'originator' => trim((string) ($decoded['originator'] ?? '')) ?: null,
            'summary' => trim((string) ($decoded['summary'] ?? '')) ?: null,
        ],
        'model' => $json['model'] ?? APPLICATION_DOC_CLASSIFIER_MODEL,
    ];
}

function application_doc_persist_pdf(string $inputPdfPath, ?string $sha256, string $originalFilename): array
{
    if (!is_dir(APPLICATION_DOC_UPLOAD_DIR) && !@mkdir(APPLICATION_DOC_UPLOAD_DIR, 0775, true) && !is_dir(APPLICATION_DOC_UPLOAD_DIR)) {
        throw new RuntimeException('Failed to create application-document uploads directory.');
    }

    $safeOriginalFilename = application_doc_safe_filename($originalFilename);
    $storedName = sprintf(
        '%s_%s_%s',
        date('Ymd_His'),
        substr((string) ($sha256 ?: sha1($safeOriginalFilename . microtime(true))), 0, 12),
        $safeOriginalFilename
    );
    $storedPath = rtrim(APPLICATION_DOC_UPLOAD_DIR, '/') . '/' . $storedName;

    if (!@copy($inputPdfPath, $storedPath)) {
        throw new RuntimeException('Failed to persist application-document PDF.');
    }
    @chmod($storedPath, 0664);

    return [
        'stored_name' => $storedName,
        'source_file' => APPLICATION_DOC_SOURCE_SUBDIR . '/' . $storedName,
    ];
}

function application_doc_upsert(PDO $pg, array $docRow): string
{
    $sql = "
        INSERT INTO public.documents (
            source_file, sha256, bytes, mime_type, pages, title,
            application_ref, document_type, local_authority, originator,
            document_date, meta, provenance, full_text, doc_vec, token_count,
            lpa_code, original_filename, site_point
        ) VALUES (
            :source_file, :sha256, :bytes, :mime_type, NULL, :title,
            :application_ref, 'application_doc', :local_authority, :originator,
            NULL, CAST(:meta AS jsonb), CAST(:provenance AS jsonb), :full_text, NULL, :token_count,
            NULL, :original_filename, NULL
        )
        ON CONFLICT (sha256) WHERE sha256 IS NOT NULL
        DO UPDATE SET
            source_file = EXCLUDED.source_file,
            bytes = EXCLUDED.bytes,
            mime_type = EXCLUDED.mime_type,
            title = EXCLUDED.title,
            application_ref = EXCLUDED.application_ref,
            document_type = EXCLUDED.document_type,
            local_authority = COALESCE(EXCLUDED.local_authority, public.documents.local_authority),
            originator = COALESCE(EXCLUDED.originator, public.documents.originator),
            meta = EXCLUDED.meta,
            provenance = EXCLUDED.provenance,
            full_text = EXCLUDED.full_text,
            token_count = EXCLUDED.token_count,
            original_filename = EXCLUDED.original_filename,
            updated_at = NOW()
        RETURNING id::text
    ";
    $st = $pg->prepare($sql);
    $st->execute($docRow);
    $docId = $st->fetchColumn();
    if (!is_string($docId) || $docId === '') {
        throw new RuntimeException('Failed to persist application document.');
    }
    return $docId;
}

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Wrong SAPI\n");
    exit(2);
}

$cli = application_doc_parse_cli($argv);
$jobId = (int) ($cli['job_id'] ?? 0);
$raw = $cli['raw'] ?? [];
$requestJsonPath = (string) ($raw['request_json_path'] ?? '');

if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job\n");
    exit(2);
}

try {
    application_doc_require_file($requestJsonPath, 'Application-document request JSON is missing.');
    $requestRaw = file_get_contents($requestJsonPath);
    $request = json_decode((string) $requestRaw, true);
    if (!is_array($request)) {
        throw new RuntimeException('Application-document request JSON is invalid.');
    }

    $jobNumber = strtoupper(trim((string) ($request['job_number'] ?? '')));
    $jobNumber = preg_replace('/\s+/', '', $jobNumber) ?? $jobNumber;
    $siteAddress = trim((string) ($request['site_address'] ?? ''));
    $client = trim((string) ($request['client'] ?? ''));
    $title = trim((string) ($request['title'] ?? ''));
    $documentRole = trim((string) ($request['document_role'] ?? ''));
    $recipientName = trim((string) ($request['recipient_name'] ?? ''));
    $extractedText = trim((string) ($request['extracted_text'] ?? ''));
    $doclingJobId = (int) ($request['docling_job_id'] ?? 0);
    $inputPdfPath = trim((string) ($request['input_pdf_path'] ?? ''));
    $originalFilename = trim((string) ($request['original_filename'] ?? 'application_doc.pdf'));

    if ($jobNumber === '' || $extractedText === '') {
        throw new RuntimeException('Missing required application-document fields: job_number and extracted_text.');
    }
    application_doc_require_file($inputPdfPath, 'Input PDF is missing.');

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 20,
        'current_action' => 'classifying application document',
    ]);

    $classification = application_doc_classify($extractedText, $title, $documentRole);
    $classificationData = is_array($classification['classification'] ?? null) ? $classification['classification'] : [];

    if ($title === '') {
        $title = trim((string) ($classificationData['title_hint'] ?? ''));
    }
    if ($title === '') {
        $title = preg_replace('/\.pdf$/i', '', $originalFilename) ?? $originalFilename;
    }
    if ($documentRole === '') {
        $documentRole = trim((string) ($classificationData['document_kind'] ?? ''));
    }
    if ($documentRole === '') {
        $documentRole = 'Supporting Document';
    }

    $sha256 = @hash_file('sha256', $inputPdfPath) ?: null;
    $bytes = @filesize($inputPdfPath);
    $bytes = $bytes !== false ? (int) $bytes : null;

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 55,
        'current_action' => 'persisting application-document PDF',
    ]);

    $stored = application_doc_persist_pdf($inputPdfPath, $sha256, $originalFilename);

    $originator = trim((string) ($classificationData['originator'] ?? '')) ?: null;
    $meta = [
        'job_number' => $jobNumber,
        'job_code' => $jobNumber,
        'reference' => $jobNumber,
        'document_title' => $title,
        'document_role' => $documentRole,
        'site_address' => $siteAddress !== '' ? $siteAddress : null,
        'client' => $client !== '' ? $client : null,
        'original_filename' => $originalFilename,
        'source_mode' => 'guided_letter_docling',
        'docling_job_id' => $doclingJobId > 0 ? $doclingJobId : null,
        'classification_model' => $classification['model'] ?? null,
        'classification_title_hint' => $classificationData['title_hint'] ?? null,
        'classification_document_kind' => $classificationData['document_kind'] ?? null,
        'classification_originator' => $classificationData['originator'] ?? null,
        'classification_summary' => $classificationData['summary'] ?? null,
    ];
    $provenance = [[
        'type' => APPLICATION_DOC_PROVENANCE_TYPE,
        'uploaded_at' => gmdate('c'),
        'original_filename' => $originalFilename,
        'docling_job_id' => $doclingJobId > 0 ? $doclingJobId : null,
    ]];

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 80,
        'current_action' => 'saving application document',
    ]);

    $docId = application_doc_upsert(application_doc_pg_pdo(), [
        ':source_file' => $stored['source_file'],
        ':sha256' => $sha256,
        ':bytes' => $bytes,
        ':mime_type' => 'application/pdf',
        ':title' => $title,
        ':application_ref' => $jobNumber,
        ':local_authority' => $recipientName !== '' ? $recipientName : null,
        ':originator' => $originator,
        ':meta' => json_encode($meta, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        ':provenance' => json_encode($provenance, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        ':full_text' => $extractedText,
        ':token_count' => application_doc_token_count($extractedText),
        ':original_filename' => $originalFilename,
    ]);

    @unlink($requestJsonPath);

    $payload = [
        'success' => true,
        'doc_id' => $docId,
        'job_number' => $jobNumber,
        'title' => $title,
        'document_role' => $documentRole,
        'document_type' => 'application_doc',
        'source_file' => $stored['source_file'],
        'original_filename' => $originalFilename,
        'classification' => $classificationData,
        'classification_model' => $classification['model'] ?? null,
    ];
    docling_job_finish($jobId, $payload, 'completed');
    exit(0);
} catch (Throwable $e) {
    application_doc_fail($jobId, $e->getMessage(), [], 1);
}
