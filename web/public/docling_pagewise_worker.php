<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const DOCLING_PYTHON = '/usr/bin/python3';
const DOCLING_PHP = '/usr/bin/php';
const DOCLING_SCRIPT = '/opt/scraper/scripts/docling_extract_pagewise.py';
const DOCLING_MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024;
const DOCLING_BATCH_SIZE = 5;
const DOCLING_HOME = '/home/james';
const DOCLING_USER_SITE = '/home/james/.local/lib/python3.12/site-packages';
const DOCLING_SHARED_CACHE_DIR = '/tmp/docling-shared-cache';
const DOCLING_POLICY_INGEST_WORKER = '/var/www/html/policy_doc_ingest_worker_local.php';
const DOCLING_APPLICATION_DOC_INGEST_WORKER = '/var/www/html/application_doc_ingest_worker_local.php';

function worker_guess_policy_metadata(string $snippet): array
{
    $apiKey = (string) docling_env('OPENAI_API_KEY', '');
    if ($apiKey === '') {
        throw new RuntimeException('OPENAI_API_KEY is not configured.');
    }

    $payload = [
        'model' => 'gpt-4o-mini',
        'temperature' => 0,
        'response_format' => ['type' => 'json_object'],
        'messages' => [
            [
                'role' => 'system',
                'content' => 'You identify policy/planning document metadata from extracted text. Return only valid JSON.',
            ],
            [
                'role' => 'user',
                'content' => implode("\n", [
                    'From the text below, identify the most likely document title, the organisation that authored or issued it, and the document date if it is clearly stated or can be inferred with high confidence.',
                    'Return strict JSON with keys:',
                    '- title',
                    '- organisation',
                    '- document_date',
                    'Use document_date in ISO format YYYY-MM-DD, or null if unknown.',
                    'If unsure, make a best effort but do not invent extra fields.',
                    '',
                    'TEXT:',
                    mb_substr($snippet, 0, 2000),
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
        throw new RuntimeException('OpenAI request failed: ' . $err);
    }

    $json = json_decode((string) $raw, true);
    if (!is_array($json)) {
        throw new RuntimeException('OpenAI returned invalid JSON.');
    }
    if ($status >= 400 || isset($json['error'])) {
        throw new RuntimeException('OpenAI returned an error.');
    }

    $content = trim((string) ($json['choices'][0]['message']['content'] ?? ''));
    $content = preg_replace('/^```(?:json)?\s*/i', '', $content) ?? $content;
    $content = preg_replace('/\s*```$/', '', $content) ?? $content;
    $decoded = json_decode(trim($content), true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Assistant output was not valid JSON.');
    }

    return [
        'title' => trim((string) ($decoded['title'] ?? '')),
        'organisation' => trim((string) ($decoded['organisation'] ?? '')),
        'document_date' => preg_match('/^\d{4}-\d{2}-\d{2}$/', (string) ($decoded['document_date'] ?? ''))
            ? (string) $decoded['document_date']
            : null,
        'model' => (string) ($json['model'] ?? 'gpt-4o-mini'),
    ];
}

function worker_run_cli_worker(string $workerPhp, array $args, string $cwd): array
{
    $parts = [escapeshellarg(is_executable(DOCLING_PHP) ? DOCLING_PHP : PHP_BINARY), escapeshellarg($workerPhp)];
    foreach ($args as $key => $value) {
        if ($value === null || $value === '') {
            continue;
        }
        $parts[] = '--' . $key . '=' . escapeshellarg((string) $value);
    }

    $proc = @proc_open(
        implode(' ', $parts),
        [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ],
        $pipes,
        $cwd
    );
    if (!is_resource($proc)) {
        throw new RuntimeException('Failed to start policy ingest worker.');
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $exitCode = proc_close($proc);

    return [
        'exit_code' => $exitCode,
        'stdout' => trim((string) $stdout),
        'stderr' => trim((string) $stderr),
    ];
}

function worker_run_policy_ingest(
    int $jobId,
    int $userId,
    string $jobDir,
    string $text,
    int $pageCount,
    ?string $sourceUrl,
    string $sourceMode,
    ?string $originalFilename,
    ?string $sourceSha256
): array {
    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 96,
        'current_action' => 'guessing policy metadata',
    ]);

    $guess = worker_guess_policy_metadata(mb_substr($text, 0, 4000));
    $title = trim((string) ($guess['title'] ?? ''));
    $organisation = trim((string) ($guess['organisation'] ?? ''));
    $documentDate = preg_match('/^\d{4}-\d{2}-\d{2}$/', (string) ($guess['document_date'] ?? ''))
        ? (string) $guess['document_date']
        : null;
    if ($title === '' || $organisation === '') {
        throw new RuntimeException('Failed to derive policy metadata.');
    }

    $requestPath = $jobDir . '/policy_ingest_request.json';
    $requestPayload = [
        'title' => $title,
        'organisation' => $organisation,
        'document_date' => $documentDate,
        'extracted_text' => $text,
        'source_mode' => $sourceMode,
        'source_url' => $sourceUrl ?: '',
        'original_filename' => $originalFilename ?: '',
        'docling_job_id' => $jobId,
        'docling_page_count' => $pageCount,
        'docling_source_sha256' => $sourceSha256 ?: '',
        'user_id' => $userId > 0 ? $userId : null,
        'classification_model' => (string) ($guess['model'] ?? 'gpt-4o-mini'),
    ];
    if (@file_put_contents($requestPath, json_encode($requestPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)) === false) {
        throw new RuntimeException('Failed to stage policy ingest request.');
    }

    $policyJobFields = [
        'kind' => 'policy/save_document',
        'status' => 'queued',
        'percentage_complete' => 0,
        'current_action' => 'queued',
    ];
    if ($userId > 0) {
        $policyJobFields['user_id'] = $userId;
    }
    $policyJobId = docling_job_create($policyJobFields);

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 97,
        'current_action' => 'ingesting policy document',
    ]);
    worker_log_json($jobId, 'policy_ingest_start', [
        'policy_job_id' => $policyJobId,
        'title' => $title,
        'organisation' => $organisation,
        'document_date' => $documentDate,
    ]);

    $result = worker_run_cli_worker(DOCLING_POLICY_INGEST_WORKER, [
        'job' => $policyJobId,
        'endpoint' => 'policy/save_document',
        'request_json_path' => $requestPath,
        'user_id' => $userId > 0 ? $userId : null,
    ], $jobDir);

    $policyJob = docling_job_get($policyJobId);
    $policyOutput = is_array($policyJob['final_output'] ?? null) ? $policyJob['final_output'] : null;
    $policyStatus = (string) ($policyJob['status'] ?? '');

    worker_log_json($jobId, 'policy_ingest_finish', [
        'policy_job_id' => $policyJobId,
        'policy_job_status' => $policyStatus,
        'exit_code' => $result['exit_code'],
        'stdout_tail' => mb_substr((string) $result['stdout'], -1000),
        'stderr_tail' => mb_substr((string) $result['stderr'], -1000),
    ]);

    if ($result['exit_code'] !== 0 || $policyStatus !== 'completed' || !is_array($policyOutput) || empty($policyOutput['doc_id'])) {
        $message = trim((string) ($policyJob['error_message'] ?? ''));
        throw new RuntimeException($message !== '' ? $message : 'Policy ingest worker failed.');
    }

    return [
        'policy_job_id' => $policyJobId,
        'policy_output' => $policyOutput,
        'title' => $title,
        'organisation' => $organisation,
        'document_date' => $documentDate,
        'classification_model' => (string) ($guess['model'] ?? 'gpt-4o-mini'),
    ];
}

function worker_run_application_doc_ingest(
    int $jobId,
    int $userId,
    string $jobDir,
    string $text,
    ?string $sourceUrl,
    string $sourceMode,
    ?string $originalFilename,
    ?string $sourceSha256,
    string $inputPdfPath,
    string $jobNumber,
    string $siteAddress,
    string $client,
    string $recipientName = '',
    string $title = '',
    string $documentRole = ''
): array {
    if (trim($jobNumber) === '') {
        throw new RuntimeException('Application-document ingest requires a job number.');
    }

    $requestPath = $jobDir . '/application_doc_ingest_request.json';
    $requestPayload = [
        'job_number' => strtoupper(trim($jobNumber)),
        'site_address' => $siteAddress,
        'client' => $client,
        'recipient_name' => $recipientName,
        'title' => $title,
        'document_role' => $documentRole,
        'extracted_text' => $text,
        'source_mode' => $sourceMode,
        'source_url' => $sourceUrl ?: '',
        'original_filename' => $originalFilename ?: '',
        'input_pdf_path' => $inputPdfPath,
        'docling_job_id' => $jobId,
        'docling_source_sha256' => $sourceSha256 ?: '',
        'user_id' => $userId > 0 ? $userId : null,
    ];
    if (@file_put_contents($requestPath, json_encode($requestPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)) === false) {
        throw new RuntimeException('Failed to stage application-document ingest request.');
    }

    $applicationJobFields = [
        'kind' => 'letters/save_application_doc',
        'status' => 'queued',
        'percentage_complete' => 0,
        'current_action' => 'queued',
    ];
    if ($userId > 0) {
        $applicationJobFields['user_id'] = $userId;
    }
    $applicationJobId = docling_job_create($applicationJobFields);

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 97,
        'current_action' => 'ingesting application document',
    ]);
    worker_log_json($jobId, 'application_doc_ingest_start', [
        'application_job_id' => $applicationJobId,
        'job_number' => $jobNumber,
        'title' => $title,
        'document_role' => $documentRole,
    ]);

    $result = worker_run_cli_worker(DOCLING_APPLICATION_DOC_INGEST_WORKER, [
        'job' => $applicationJobId,
        'endpoint' => 'letters/save_application_doc',
        'request_json_path' => $requestPath,
        'user_id' => $userId > 0 ? $userId : null,
    ], $jobDir);

    $applicationJob = docling_job_get($applicationJobId);
    $applicationOutput = is_array($applicationJob['final_output'] ?? null) ? $applicationJob['final_output'] : null;
    $applicationStatus = (string) ($applicationJob['status'] ?? '');

    worker_log_json($jobId, 'application_doc_ingest_finish', [
        'application_job_id' => $applicationJobId,
        'application_job_status' => $applicationStatus,
        'exit_code' => $result['exit_code'],
        'stdout_tail' => mb_substr((string) $result['stdout'], -1000),
        'stderr_tail' => mb_substr((string) $result['stderr'], -1000),
    ]);

    if ($result['exit_code'] !== 0 || $applicationStatus !== 'completed' || !is_array($applicationOutput) || empty($applicationOutput['doc_id'])) {
        $message = trim((string) ($applicationJob['error_message'] ?? ''));
        throw new RuntimeException($message !== '' ? $message : 'Application-document ingest worker failed.');
    }

    return [
        'application_job_id' => $applicationJobId,
        'application_output' => $applicationOutput,
        'title' => trim((string) ($applicationOutput['title'] ?? $title)),
        'document_role' => trim((string) ($applicationOutput['document_role'] ?? $documentRole)),
        'classification_model' => (string) ($applicationOutput['classification_model'] ?? ''),
    ];
}

function worker_arg(string $name, ?string $default = null): ?string
{
    global $argv;
    foreach (array_slice($argv, 1) as $arg) {
        if (!str_starts_with($arg, '--')) {
            continue;
        }
        $pos = strpos($arg, '=');
        if ($pos === false) {
            continue;
        }
        $key = substr($arg, 2, $pos - 2);
        if ($key === $name) {
            return substr($arg, $pos + 1);
        }
    }
    return $default;
}

function worker_fail_and_exit(int $jobId, string $message, ?array $payload = null, int $code = 1): never
{
    docling_worker_log($jobId, 'ERROR ' . $message);
    docling_job_fail($jobId, $message, $payload);
    exit($code);
}

function worker_memory_snapshot(): array
{
    return [
        'usage_mb' => round(memory_get_usage(true) / 1048576, 2),
        'peak_mb' => round(memory_get_peak_usage(true) / 1048576, 2),
    ];
}

function worker_log_json(int $jobId, string $event, array $payload = []): void
{
    docling_worker_log($jobId, json_encode(
        ['event' => $event] + $payload,
        JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_IGNORE
    ));
}

function worker_update_progress(int $jobId, int $pageDone, int $pageTotal, string $action): void
{
    $ratio = $pageTotal > 0 ? min(1, max(0, $pageDone / $pageTotal)) : 0;
    $pc = 10 + (int) floor($ratio * 85);
    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => min(99, $pc),
        'current_action' => $action,
    ]);
}

function worker_has_cmd(string $cmd): bool
{
    $out = [];
    $ret = 0;
    @exec('command -v ' . escapeshellarg($cmd) . ' 2>/dev/null', $out, $ret);
    return $ret === 0 && !empty($out);
}

function worker_pdf_page_count(string $pdfPath): ?int
{
    if (!worker_has_cmd('pdfinfo')) {
        return null;
    }
    $out = [];
    $ret = 0;
    exec('pdfinfo ' . escapeshellarg($pdfPath) . ' 2>/dev/null', $out, $ret);
    if ($ret !== 0) {
        return null;
    }
    foreach ($out as $line) {
        if (stripos($line, 'Pages:') === 0) {
            return (int) trim(substr($line, 6));
        }
    }
    return null;
}

function worker_is_private_ip(string $ip): bool
{
    if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
        $long = ip2long($ip);
        $ranges = [
            ['10.0.0.0',   '10.255.255.255'],
            ['172.16.0.0', '172.31.255.255'],
            ['192.168.0.0','192.168.255.255'],
            ['127.0.0.0',  '127.255.255.255'],
        ];
        foreach ($ranges as [$start, $end]) {
            if ($long >= ip2long($start) && $long <= ip2long($end)) {
                return true;
            }
        }
        return false;
    }
    if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
        $low = strtolower($ip);
        return $low === '::1' || str_starts_with($low, 'fc') || str_starts_with($low, 'fd') || str_starts_with($low, 'fe80');
    }
    return false;
}

function worker_download_pdf(string $url, string $dest): array
{
    $parts = parse_url($url);
    if (!$parts || !in_array(strtolower($parts['scheme'] ?? ''), ['http', 'https'], true)) {
        return ['ok' => false, 'error' => 'Only http/https URLs are allowed'];
    }
    $host = (string) ($parts['host'] ?? '');
    $ip = @gethostbyname($host);
    if ($ip !== '' && filter_var($ip, FILTER_VALIDATE_IP) && worker_is_private_ip($ip)) {
        return ['ok' => false, 'error' => 'Refusing to fetch private/loopback host'];
    }

    $fh = @fopen($dest, 'wb');
    if (!$fh) {
        return ['ok' => false, 'error' => 'Cannot open temp file for writing'];
    }

    $downloaded = 0;
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS => 5,
        CURLOPT_CONNECTTIMEOUT => 20,
        CURLOPT_TIMEOUT => 600,
        CURLOPT_FILE => $fh,
        CURLOPT_NOPROGRESS => false,
        CURLOPT_USERAGENT => 'docling-pagewise-worker/1.0',
        CURLOPT_PROGRESSFUNCTION => function ($resource, $dlTotal, $dlNow, $ulTotal = 0, $ulNow = 0) use (&$downloaded) {
            $downloaded = (int) $dlNow;
            return ($dlNow > DOCLING_MAX_DOWNLOAD_BYTES) ? 1 : 0;
        },
        CURLOPT_HTTPHEADER => ['Expect:'],
    ]);
    $ok = curl_exec($ch);
    $err = curl_error($ch);
    $http = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    fflush($fh);
    fclose($fh);

    if (!$ok) {
        @unlink($dest);
        return ['ok' => false, 'error' => 'cURL error: ' . $err, 'http' => $http];
    }
    if ($http < 200 || $http >= 300) {
        @unlink($dest);
        return ['ok' => false, 'error' => 'HTTP ' . $http . ' while fetching URL', 'http' => $http];
    }

    $fh = @fopen($dest, 'rb');
    $sig = $fh ? (string) fread($fh, 5) : '';
    if ($fh) {
        fclose($fh);
    }
    if ($sig !== '%PDF-') {
        @unlink($dest);
        return ['ok' => false, 'error' => 'Downloaded file is not a valid PDF'];
    }

    return ['ok' => true, 'bytes' => $downloaded ?: (@filesize($dest) ?: 0)];
}

function worker_docling_base_cmd(string $file, string $pagesDir, string $mergedOutput, string $cacheDir, int $sleepMs, bool $stopOnError, string $extractMode): array
{
    $cmd = [
        'HOME=' . escapeshellarg(DOCLING_HOME),
        'PYTHONPATH=' . escapeshellarg(DOCLING_USER_SITE . (getenv('PYTHONPATH') ? PATH_SEPARATOR . getenv('PYTHONPATH') : '')),
        'PYTHONUNBUFFERED=1',
        'XDG_CACHE_HOME=' . escapeshellarg($cacheDir),
        'HF_HOME=' . escapeshellarg($cacheDir . '/huggingface'),
        'TRANSFORMERS_CACHE=' . escapeshellarg($cacheDir . '/huggingface/transformers'),
        escapeshellarg(DOCLING_PYTHON),
        escapeshellarg(DOCLING_SCRIPT),
        escapeshellarg($file),
        '--extract-mode', escapeshellarg($extractMode),
        '--out-dir', escapeshellarg($pagesDir),
        '--merged-output', escapeshellarg($mergedOutput),
    ];
    if ($sleepMs > 0) {
        $cmd[] = '--sleep-ms';
        $cmd[] = (string) $sleepMs;
    }
    if ($stopOnError) {
        $cmd[] = '--stop-on-error';
    }
    return $cmd;
}

function worker_run_docling_batch(
    int $jobId,
    array $cmd,
    string $jobDir,
    string $modeLabel,
    int $targetPages,
    int &$donePages,
    int &$failedPages,
    array &$stderrTail,
    array &$stdoutTail
): array {
    $proc = @proc_open(
        implode(' ', $cmd),
        [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ],
        $pipes,
        $jobDir
    );

    if (!is_resource($proc)) {
        throw new RuntimeException('Failed to start Docling Python process.');
    }

    fclose($pipes[0]);
    stream_set_blocking($pipes[1], false);
    stream_set_blocking($pipes[2], false);

    $stdoutBuf = '';
    $stderrBuf = '';
    $summary = null;

    $handleLine = function (string $line, bool $isErr) use (&$donePages, &$failedPages, $targetPages, $jobId, &$summary, &$stderrTail, &$stdoutTail, $modeLabel): void {
        $line = trim($line);
        if ($line === '') {
            return;
        }
        if ($isErr) {
            $stderrTail[] = $line;
            $stderrTail = array_slice($stderrTail, -50);
        } else {
            $stdoutTail[] = $line;
            $stdoutTail = array_slice($stdoutTail, -50);
        }

        if (preg_match('/page=(\d+)\s+start/i', $line, $m)) {
            $page = (int) $m[1];
            worker_update_progress($jobId, max(0, $donePages + $failedPages), $targetPages, "{$modeLabel} page {$page} started");
            return;
        }
        if (preg_match('/page=(\d+)\s+ok/i', $line, $m)) {
            $page = (int) $m[1];
            $donePages++;
            worker_update_progress($jobId, $donePages + $failedPages, $targetPages, "{$modeLabel} page {$page} extracted");
            return;
        }
        if (preg_match('/page=(\d+)\s+failed/i', $line, $m)) {
            $page = (int) $m[1];
            $failedPages++;
            worker_update_progress($jobId, $donePages + $failedPages, $targetPages, "{$modeLabel} page {$page} failed");
            return;
        }
        if ($line[0] === '{') {
            $json = json_decode($line, true);
            if (is_array($json) && array_key_exists('pages_ok', $json)) {
                $summary = $json;
            }
        }
    };

    while (true) {
        $status = proc_get_status($proc);
        $running = (bool) ($status['running'] ?? false);

        $read = [];
        if (!feof($pipes[1])) {
            $read[] = $pipes[1];
        }
        if (!feof($pipes[2])) {
            $read[] = $pipes[2];
        }
        if ($read) {
            $write = null;
            $except = null;
            @stream_select($read, $write, $except, 1, 0);
            foreach ($read as $stream) {
                $chunk = (string) fread($stream, 8192);
                if ($chunk === '') {
                    continue;
                }
                if ($stream === $pipes[1]) {
                    $stdoutBuf .= $chunk;
                    while (($pos = strpos($stdoutBuf, "\n")) !== false) {
                        $line = substr($stdoutBuf, 0, $pos);
                        $stdoutBuf = substr($stdoutBuf, $pos + 1);
                        $handleLine($line, false);
                    }
                } else {
                    $stderrBuf .= $chunk;
                    while (($pos = strpos($stderrBuf, "\n")) !== false) {
                        $line = substr($stderrBuf, 0, $pos);
                        $stderrBuf = substr($stderrBuf, $pos + 1);
                        $handleLine($line, true);
                    }
                }
            }
        }

        if (!$running) {
            break;
        }
    }

    if (trim($stdoutBuf) !== '') {
        $handleLine($stdoutBuf, false);
    }
    if (trim($stderrBuf) !== '') {
        $handleLine($stderrBuf, true);
    }

    fclose($pipes[1]);
    fclose($pipes[2]);

    return [
        'exit_code' => proc_close($proc),
        'summary' => $summary,
    ];
}

$jobId = (int) worker_arg('job', '0');
if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job\n");
    exit(2);
}

$sourceMode = worker_arg('source-mode', 'upload');
$file = worker_arg('file');
$pdfUrl = worker_arg('pdf-url');
$startPage = max(1, (int) worker_arg('start-page', '1'));
$endPageRaw = worker_arg('end-page');
$endPage = ($endPageRaw !== null && $endPageRaw !== '') ? (int) $endPageRaw : null;
$sleepMs = max(0, (int) worker_arg('sleep-ms', '0'));
$stopOnError = docling_boolish(worker_arg('stop-on-error', '0'));
$ingestAfterExtract = docling_boolish(worker_arg('ingest-after-extract', '1'));
$ingestKind = trim((string) worker_arg('ingest-kind', 'policy_document'));
$userId = max(0, (int) worker_arg('user-id', '0'));
$originalFilename = trim((string) worker_arg('original-filename', ''));
$jobNumber = trim((string) worker_arg('job-number', ''));
$siteAddress = trim((string) worker_arg('site-address', ''));
$client = trim((string) worker_arg('client', ''));
$recipientName = trim((string) worker_arg('recipient-name', ''));
$documentTitle = trim((string) worker_arg('title', ''));
$documentRole = trim((string) worker_arg('document-role', ''));
$extractMode = strtolower(trim((string) worker_arg('extract-mode', 'docling')));
if (!in_array($extractMode, ['docling', 'pdftotext'], true)) {
    $extractMode = 'docling';
}
$modeLabel = $extractMode === 'pdftotext' ? 'pdftotext' : 'Docling';

$jobDir = docling_job_dir($jobId);
$pagesDir = $jobDir . '/pages';
$mergedOutput = $jobDir . '/merged.txt';
$cacheDir = DOCLING_SHARED_CACHE_DIR;
if (!is_dir($pagesDir)) {
    @mkdir($pagesDir, 0777, true);
}
@chmod($pagesDir, 0777);
if (!is_dir($cacheDir)) {
    @mkdir($cacheDir, 0777, true);
}
@chmod($cacheDir, 0777);
if (!is_dir($cacheDir . '/huggingface')) {
    @mkdir($cacheDir . '/huggingface', 0777, true);
}
@chmod($cacheDir . '/huggingface', 0777);
if (is_file($mergedOutput)) {
    @chmod($mergedOutput, 0666);
}

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 2,
    'current_action' => $sourceMode === 'url' ? 'fetching source PDF' : 'preparing source PDF',
]);

if ($sourceMode === 'url') {
    if (!$pdfUrl) {
        worker_fail_and_exit($jobId, 'Missing --pdf-url for url mode.');
    }
    $file = $jobDir . '/input.pdf';
    $dl = worker_download_pdf($pdfUrl, $file);
    if (empty($dl['ok'])) {
        worker_fail_and_exit($jobId, 'Failed to fetch pdf_url: ' . ($dl['error'] ?? 'unknown'));
    }
    docling_worker_log($jobId, 'Downloaded PDF from URL.');
} elseif (!$file || !is_readable($file)) {
    worker_fail_and_exit($jobId, 'Input PDF is missing or unreadable.');
}

$pageCount = worker_pdf_page_count($file);
if ($pageCount !== null && $endPage === null) {
    $endPage = $pageCount;
}
if ($endPage === null) {
    worker_fail_and_exit($jobId, 'Could not determine page count; provide end_page explicitly.');
}
if ($pageCount !== null && $endPage > $pageCount) {
    $endPage = $pageCount;
}
if ($endPage < $startPage) {
    worker_fail_and_exit($jobId, 'end_page must be >= start_page.');
}

$targetPages = $endPage - $startPage + 1;
$sha256 = @hash_file('sha256', $file) ?: null;
$bytes = @filesize($file) ?: null;

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 5,
    'current_action' => sprintf('starting %s (%d page%s)', $modeLabel, $targetPages, $targetPages === 1 ? '' : 's'),
]);

$baseCmd = worker_docling_base_cmd($file, $pagesDir, $mergedOutput, $cacheDir, $sleepMs, $stopOnError, $extractMode);
$donePages = 0;
$failedPages = 0;
$stderrTail = [];
$stdoutTail = [];
$summaries = [];
$exitCode = 0;
$batchIndex = 0;
for ($batchStart = $startPage; $batchStart <= $endPage; $batchStart += DOCLING_BATCH_SIZE, $batchIndex++) {
    $batchEnd = min($endPage, $batchStart + DOCLING_BATCH_SIZE - 1);
    $cmd = $baseCmd;
    $cmd[] = '--start-page';
    $cmd[] = (string) $batchStart;
    $cmd[] = '--end-page';
    $cmd[] = (string) $batchEnd;
    if ($batchIndex > 0) {
        $cmd[] = '--append-merged';
    }

    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => min(99, max(5, 10 + (int) floor((($donePages + $failedPages) / max(1, $targetPages)) * 85))),
        'current_action' => sprintf('starting %s batch %d-%d', $modeLabel, $batchStart, $batchEnd),
    ]);
    worker_log_json($jobId, 'batch_start', [
        'batch_start_page' => $batchStart,
        'batch_end_page' => $batchEnd,
        'pages_done' => $donePages,
        'pages_failed' => $failedPages,
        'memory' => worker_memory_snapshot(),
    ]);
    docling_worker_log($jobId, 'Launching batch command: ' . implode(' ', $cmd));

    try {
        $result = worker_run_docling_batch($jobId, $cmd, $jobDir, $modeLabel, $targetPages, $donePages, $failedPages, $stderrTail, $stdoutTail);
    } catch (Throwable $e) {
        worker_fail_and_exit($jobId, $e->getMessage());
    }

    $exitCode = (int) ($result['exit_code'] ?? 1);
    $summary = is_array($result['summary'] ?? null) ? $result['summary'] : null;
    if ($summary !== null) {
        $summaries[] = $summary;
    }
    worker_log_json($jobId, 'batch_finish', [
        'batch_start_page' => $batchStart,
        'batch_end_page' => $batchEnd,
        'exit_code' => $exitCode,
        'summary_pages_ok' => $summary['pages_ok'] ?? null,
        'summary_pages_failed' => $summary['pages_failed'] ?? null,
        'pages_done' => $donePages,
        'pages_failed' => $failedPages,
        'stdout_tail' => array_slice($stdoutTail, -10),
        'stderr_tail' => array_slice($stderrTail, -10),
        'memory' => worker_memory_snapshot(),
    ]);

    $batchMadeProgress = $summary !== null
        ? (((int) ($summary['pages_ok'] ?? 0)) + ((int) ($summary['pages_failed'] ?? 0)) > 0)
        : false;
    if ($exitCode !== 0 && !$batchMadeProgress) {
        worker_fail_and_exit($jobId, 'Docling extraction failed before any batch output was produced.', [
            'stdout_tail' => $stdoutTail,
            'stderr_tail' => $stderrTail,
            'exit_code' => $exitCode,
            'batch_start_page' => $batchStart,
            'batch_end_page' => $batchEnd,
        ], $exitCode);
    }
    if ($stopOnError && $summary !== null && (int) ($summary['pages_failed'] ?? 0) > 0) {
        break;
    }
}

$text = is_file($mergedOutput) ? (string) file_get_contents($mergedOutput) : '';
$allErrors = [];
foreach ($summaries as $batchSummary) {
    foreach (($batchSummary['errors'] ?? []) as $errorRow) {
        $allErrors[] = $errorRow;
    }
}
$summary = $summaries !== [] ? end($summaries) : [
    'ok' => $exitCode === 0,
    'pdf' => $file,
    'start_page' => $startPage,
    'end_page' => $endPage,
    'out_dir' => $pagesDir,
    'merged_output' => $mergedOutput,
    'pages_ok' => 0,
    'pages_failed' => 0,
    'errors' => [],
];
$pagesOk = $donePages;
$pagesFailed = $failedPages;
$success = trim($text) !== '' && $pagesOk > 0;
$partial = $success && $pagesFailed > 0;

$payload = [
    'success' => $success,
    'partial' => $partial,
    'job_id' => $jobId,
    'source_mode' => $sourceMode,
    'extract_mode' => $extractMode,
    'pdf_url' => $sourceMode === 'url' ? $pdfUrl : null,
    'original_filename' => $originalFilename !== '' ? $originalFilename : basename($file ?: 'policy_document.pdf'),
    'file' => $file,
    'sha256' => $sha256,
    'bytes' => $bytes,
    'page_count' => $pageCount,
    'start_page' => $startPage,
    'end_page' => $endPage,
    'pages_ok' => $pagesOk,
    'pages_failed' => $pagesFailed,
    'errors' => $allErrors,
    'out_dir' => $pagesDir,
    'merged_output' => $mergedOutput,
    'text' => $text,
    'stdout_tail' => $stdoutTail,
    'stderr_tail' => $stderrTail,
    'exit_code' => $exitCode,
];

if (!$success) {
    worker_log_json($jobId, 'job_finish_failure', [
        'pages_ok' => $pagesOk,
        'pages_failed' => $pagesFailed,
        'exit_code' => $exitCode,
        'stdout_tail' => array_slice($stdoutTail, -10),
        'stderr_tail' => array_slice($stderrTail, -10),
        'memory' => worker_memory_snapshot(),
    ]);
    worker_fail_and_exit(
        $jobId,
        'Extraction failed.',
        $payload,
        $exitCode === 0 ? 1 : $exitCode
    );
}

if ($ingestAfterExtract) {
    try {
        if ($ingestKind === 'application_doc') {
            $ingest = worker_run_application_doc_ingest(
                $jobId,
                $userId,
                $jobDir,
                $text,
                $pdfUrl,
                $sourceMode,
                $originalFilename !== '' ? $originalFilename : basename($file ?: 'application_document.pdf'),
                $sha256,
                (string) $file,
                $jobNumber,
                $siteAddress,
                $client,
                $recipientName,
                $documentTitle,
                $documentRole
            );
            $payload['title'] = $ingest['title'];
            $payload['document_role'] = $ingest['document_role'];
            $payload['classification_model'] = $ingest['classification_model'];
            $payload['application_save_job_id'] = $ingest['application_job_id'];
            $payload['doc_id'] = $ingest['application_output']['doc_id'] ?? null;
            $payload['document_type'] = $ingest['application_output']['document_type'] ?? 'application_doc';
            $payload['ingest_output'] = $ingest['application_output'];
        } else {
            $docPageCount = $pageCount ?? ($pagesOk + $pagesFailed);
            $ingest = worker_run_policy_ingest(
                $jobId,
                $userId,
                $jobDir,
                $text,
                $docPageCount,
                $pdfUrl,
                $sourceMode,
                $originalFilename !== '' ? $originalFilename : basename($file ?: 'policy_document.pdf'),
                $sha256
            );
            $payload['title'] = $ingest['title'];
            $payload['organisation'] = $ingest['organisation'];
            $payload['classification_model'] = $ingest['classification_model'];
            $payload['policy_save_job_id'] = $ingest['policy_job_id'];
            $payload['doc_id'] = $ingest['policy_output']['doc_id'] ?? null;
            $payload['document_type'] = $ingest['policy_output']['document_type'] ?? 'policy_document';
            $payload['ingest_output'] = $ingest['policy_output'];
        }
    } catch (Throwable $e) {
        $label = $ingestKind === 'application_doc' ? 'Application-document ingest failed: ' : 'Policy ingest failed: ';
        worker_fail_and_exit($jobId, $label . $e->getMessage(), $payload, 1);
    }
} else {
    $payload['ingest_skipped'] = true;
}

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 99,
    'current_action' => $partial ? 'completed with some fallback pages' : 'finalising output',
]);
worker_log_json($jobId, 'job_finish_success', [
    'pages_ok' => $pagesOk,
    'pages_failed' => $pagesFailed,
    'exit_code' => $exitCode,
    'memory' => worker_memory_snapshot(),
]);
docling_job_finish($jobId, $payload, 'completed');
docling_worker_log($jobId, 'Completed successfully.');
exit(0);
