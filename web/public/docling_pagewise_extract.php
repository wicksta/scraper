<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const DOCLING_WORKER_SCRIPT = '/var/www/html/docling_pagewise_worker.php';

docling_cleanup_old_job_dirs();

function docling_api_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function docling_upload_error_message(int $err): string
{
    return match ($err) {
        UPLOAD_ERR_INI_SIZE      => 'File exceeds upload_max_filesize.',
        UPLOAD_ERR_FORM_SIZE     => 'File exceeds MAX_FILE_SIZE form limit.',
        UPLOAD_ERR_PARTIAL       => 'File was only partially uploaded.',
        UPLOAD_ERR_NO_FILE       => 'No file was uploaded.',
        UPLOAD_ERR_NO_TMP_DIR    => 'Missing a temporary folder on server.',
        UPLOAD_ERR_CANT_WRITE    => 'Failed to write file to disk.',
        UPLOAD_ERR_EXTENSION     => 'A PHP extension stopped the file upload.',
        default                  => 'Unknown upload error.',
    };
}

function docling_detect_pdf_upload(array $file): bool
{
    $tmp = (string) ($file['tmp_name'] ?? '');
    if ($tmp === '' || !is_file($tmp)) {
        return false;
    }
    $fh = @fopen($tmp, 'rb');
    if (!$fh) {
        return false;
    }
    $sig = (string) fread($fh, 5);
    fclose($fh);
    if ($sig === '%PDF-') {
        return true;
    }
    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mime = $finfo ? finfo_file($finfo, $tmp) : null;
    if ($finfo) {
        finfo_close($finfo);
    }
    return strtolower((string) $mime) === 'application/pdf';
}

function docling_start_background_job(string $workerPhp, array $args, int $jobId): ?string
{
    $phpBin = '/usr/local/php82/bin/php-cli';
    if (!is_executable($phpBin)) {
        $phpBin = '/usr/bin/php';
    }

    $parts = [escapeshellarg($phpBin), escapeshellarg($workerPhp)];
    foreach ($args as $key => $value) {
        if ($value === null || $value === '') {
            continue;
        }
        if (is_bool($value)) {
            if ($value) {
                $parts[] = '--' . $key . '=1';
            }
            continue;
        }
        $parts[] = '--' . $key . '=' . escapeshellarg((string) $value);
    }
    $cmd = implode(' ', $parts);
    $jobDir = docling_job_dir($jobId);
    $logFile = $jobDir . '/spawn.log';
    $full = 'nohup ' . $cmd . ' </dev/null >> ' . escapeshellarg($logFile) . ' 2>&1 & echo $!';

    $atBin = trim((string) @shell_exec('command -v at 2>/dev/null'));
    if ($atBin !== '') {
        $active = trim((string) @shell_exec('systemctl is-active atd 2>/dev/null'));
        if ($active === 'active') {
            $atCmd = $cmd . ' >> ' . escapeshellarg($logFile) . ' 2>&1';
            $schedule = 'printf %s ' . escapeshellarg($atCmd) . ' | ' . escapeshellarg($atBin) . ' -M now 2>&1';
            $atOut = [];
            $atCode = 0;
            @exec($schedule, $atOut, $atCode);
            $joined = trim(implode("\n", $atOut));
            if ($atCode === 0) {
                if (preg_match('/job\s+(\d+)/i', $joined, $m)) {
                    @file_put_contents($logFile, '[' . gmdate('c') . '] queued via at job ' . $m[1] . PHP_EOL, FILE_APPEND);
                    return 'at:' . $m[1];
                }
                @file_put_contents($logFile, '[' . gmdate('c') . '] queued via at' . PHP_EOL, FILE_APPEND);
                return 'at';
            }
            @file_put_contents(
                $logFile,
                '[' . gmdate('c') . '] at handoff failed: ' . $joined . PHP_EOL,
                FILE_APPEND
            );
        }
    }

    if (function_exists('shell_exec')) {
        $out = @shell_exec($full);
        $pid = trim((string) $out);
        if ($pid !== '') {
            return $pid;
        }
    }

    $pid = null;
    @exec($full, $lines, $code);
    if (!empty($lines[0])) {
        $pid = trim((string) $lines[0]);
    }
    if ($pid !== null && $pid !== '') {
        return $pid;
    }

    @exec('nohup ' . $cmd . ' </dev/null >> ' . escapeshellarg($logFile) . ' 2>&1 &');
    return 'spawned';
}

docling_api_auth();

if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    $jobId = (int) ($_GET['job_id'] ?? $_GET['jobId'] ?? 0);
    $debug = docling_boolish($_GET['debug'] ?? false);
    if ($jobId <= 0) {
        docling_json_out(400, ['success' => false, 'error' => 'missing or invalid job_id', 'code' => 400]);
    }
    $job = docling_job_get($jobId);
    if (!$job) {
        docling_json_out(404, ['success' => false, 'error' => 'job not found', 'code' => 404]);
    }
    if ($debug) {
        $jobDir = docling_job_dir($jobId);
        $mergedPath = $jobDir . '/merged.txt';
        if (is_readable($mergedPath)) {
            $job['debug_text'] = (string) file_get_contents($mergedPath);
            $job['debug_text_bytes'] = strlen($job['debug_text']);
            $job['debug_merged_path'] = $mergedPath;
        }
    }
    docling_json_out(200, ['success' => true, 'job' => $job]);
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    docling_json_out(405, ['success' => false, 'error' => 'Only GET and POST are supported.', 'code' => 405]);
}

$startPage = max(1, (int) ($_POST['start_page'] ?? 1));
$endPageRaw = trim((string) ($_POST['end_page'] ?? ''));
$endPage = $endPageRaw !== '' ? (int) $endPageRaw : null;
$sleepMs = max(0, (int) ($_POST['sleep_ms'] ?? 0));
$stopOnError = docling_boolish($_POST['stop_on_error'] ?? false);
$ingestAfterExtract = !isset($_POST['ingest_after_extract']) || docling_boolish($_POST['ingest_after_extract']);
$ingestKind = trim((string) ($_POST['ingest_kind'] ?? ''));
$jobNumber = trim((string) ($_POST['job_number'] ?? ''));
$siteAddress = trim((string) ($_POST['site_address'] ?? ''));
$client = trim((string) ($_POST['client'] ?? ''));
$recipientName = trim((string) ($_POST['recipient_name'] ?? ''));
$title = trim((string) ($_POST['title'] ?? ''));
$documentRole = trim((string) ($_POST['document_role'] ?? ''));
$extractMode = strtolower(trim((string) ($_POST['extract_mode'] ?? 'docling')));
if (!in_array($extractMode, ['docling', 'pdftotext'], true)) {
    $extractMode = 'docling';
}

if ($endPage !== null && $endPage < $startPage) {
    docling_json_out(400, ['success' => false, 'error' => 'end_page must be >= start_page', 'code' => 400]);
}

$hasUpload = isset($_FILES['pdf']);
$pdfUrl = trim((string) ($_POST['pdf_url'] ?? ''));
$userId = max(0, (int) ($_POST['user_id'] ?? 0));

if (!$hasUpload && $pdfUrl === '') {
    docling_json_out(400, ['success' => false, 'error' => 'Provide either a PDF upload (pdf) or pdf_url.', 'code' => 400]);
}

$jobFields = [
    'kind' => 'docling_pagewise_extract',
    'status' => 'queued',
    'percentage_complete' => 0,
    'current_action' => 'queued',
];
if ($userId > 0) {
    $jobFields['user_id'] = $userId;
}

$jobId = docling_job_create($jobFields);

$jobDir = docling_job_dir($jobId);
$args = [
    'job' => $jobId,
    'start-page' => $startPage,
    'sleep-ms' => $sleepMs,
    'extract-mode' => $extractMode,
];
if ($userId > 0) {
    $args['user-id'] = $userId;
}
if ($endPage !== null) {
    $args['end-page'] = $endPage;
}
if ($stopOnError) {
    $args['stop-on-error'] = true;
}
if (!$ingestAfterExtract) {
    $args['ingest-after-extract'] = 0;
}
if ($ingestKind !== '') {
    $args['ingest-kind'] = $ingestKind;
}
if ($jobNumber !== '') {
    $args['job-number'] = $jobNumber;
}
if ($siteAddress !== '') {
    $args['site-address'] = $siteAddress;
}
if ($client !== '') {
    $args['client'] = $client;
}
if ($recipientName !== '') {
    $args['recipient-name'] = $recipientName;
}
if ($title !== '') {
    $args['title'] = $title;
}
if ($documentRole !== '') {
    $args['document-role'] = $documentRole;
}

if ($hasUpload) {
    $file = $_FILES['pdf'];
    $uploadError = (int) ($file['error'] ?? UPLOAD_ERR_NO_FILE);
    if ($uploadError !== UPLOAD_ERR_OK) {
        docling_job_fail($jobId, docling_upload_error_message($uploadError));
        docling_json_out(400, ['success' => false, 'error' => docling_upload_error_message($uploadError), 'job_id' => $jobId, 'code' => 400]);
    }
    if (!docling_detect_pdf_upload($file)) {
        docling_job_fail($jobId, 'Uploaded file is not a valid PDF.');
        docling_json_out(400, ['success' => false, 'error' => 'Uploaded file is not a valid PDF.', 'job_id' => $jobId, 'code' => 400]);
    }
    $dest = $jobDir . '/input.pdf';
    $tmp = (string) ($file['tmp_name'] ?? '');
    if (!@move_uploaded_file($tmp, $dest)) {
        if (!@copy($tmp, $dest)) {
            docling_job_fail($jobId, 'Failed to stage upload for worker.');
            docling_json_out(500, ['success' => false, 'error' => 'Failed to stage upload for worker.', 'job_id' => $jobId, 'code' => 500]);
        }
    }
    $args['file'] = $dest;
    $args['original-filename'] = (string) ($file['name'] ?? 'policy_document.pdf');
    $args['source-mode'] = 'upload';
    docling_job_update($jobId, [
        'current_action' => 'queued: upload staged',
    ]);
} else {
    $args['pdf-url'] = $pdfUrl;
    $args['source-mode'] = 'url';
    docling_job_update($jobId, [
        'current_action' => 'queued: awaiting URL fetch',
    ]);
}

$pid = docling_start_background_job(DOCLING_WORKER_SCRIPT, $args, $jobId);
if ($pid === null) {
    docling_job_fail($jobId, 'Failed to start Docling worker.');
    docling_json_out(500, ['success' => false, 'error' => 'Failed to start Docling worker.', 'job_id' => $jobId, 'code' => 500]);
}

docling_job_update($jobId, [
    'status' => 'running',
    'current_action' => 'worker spawned',
    'percentage_complete' => 1,
]);

docling_json_out(202, [
    'success' => true,
    'job_id' => $jobId,
    'pid' => $pid,
    'status' => 'running',
    'status_path' => basename(__FILE__) . '?job_id=' . $jobId . '&api_key=' . rawurlencode(docling_request_api_key()),
]);
