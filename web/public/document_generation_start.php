<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const DOCGEN_START_WORKER = '/opt/scraper/workers/document_generation_worker.php';

function docgen_start_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function docgen_start_fail(int $status, string $message, array $extra = []): void
{
    docling_json_out($status, ['success' => false, 'error' => $message] + $extra);
}

function docgen_start_background_job(string $workerPhp, array $args, int $jobId): ?string
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
        $parts[] = '--' . $key . '=' . escapeshellarg((string) $value);
    }

    $cmd = implode(' ', $parts);
    $jobDir = docling_job_dir($jobId);
    $logFile = $jobDir . '/spawn.log';
    $full = 'nohup ' . $cmd . ' </dev/null >> ' . escapeshellarg($logFile) . ' 2>&1 & echo $!';

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

    return null;
}

docling_cleanup_old_job_dirs();
docgen_start_auth();

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') !== 'POST') {
    docgen_start_fail(405, 'Only POST is supported.', ['code' => 405]);
}

$docType = strtolower(trim((string) ($_POST['doc_type'] ?? '')));
$notesText = trim((string) ($_POST['notes_text'] ?? ''));
$userId = max(0, (int) ($_POST['user_id'] ?? 0));

if (!in_array($docType, ['note', 'letter', 'minutes'], true)) {
    docgen_start_fail(400, 'Invalid or missing doc_type.', ['code' => 400]);
}

$hasUpload = isset($_FILES['notes_file']) && is_array($_FILES['notes_file']) && (int) ($_FILES['notes_file']['error'] ?? UPLOAD_ERR_NO_FILE) !== UPLOAD_ERR_NO_FILE;
if ($notesText === '' && !$hasUpload) {
    docgen_start_fail(400, 'Provide draft notes text or upload a .docx file.', ['code' => 400]);
}

$jobFields = [
    'kind' => 'documents/generate_structured',
    'status' => 'queued',
    'percentage_complete' => 0,
    'current_action' => 'queued',
];
if ($userId > 0) {
    $jobFields['user_id'] = $userId;
}
$jobId = docling_job_create($jobFields);
$jobDir = docling_job_dir($jobId);

$requestPayload = [
    'doc_type' => $docType,
    'notes_text' => $notesText,
    'source' => $notesText !== '' ? 'textarea' : null,
    'user_id' => $userId > 0 ? $userId : null,
];

if ($hasUpload) {
    $file = $_FILES['notes_file'];
    $uploadError = (int) ($file['error'] ?? UPLOAD_ERR_NO_FILE);
    if ($uploadError !== UPLOAD_ERR_OK) {
        docling_job_fail($jobId, 'Upload failed.', ['success' => false, 'error' => 'Upload failed.']);
        docgen_start_fail(400, 'Upload failed.', ['job_id' => $jobId, 'code' => 400]);
    }

    $originalName = (string) ($file['name'] ?? 'notes.docx');
    $ext = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
    if ($ext !== 'docx') {
        docling_job_fail($jobId, 'Only .docx uploads are supported right now.', ['success' => false, 'error' => 'Only .docx uploads are supported right now.']);
        docgen_start_fail(400, 'Only .docx uploads are supported right now.', ['job_id' => $jobId, 'code' => 400]);
    }

    $stagedPath = $jobDir . '/notes_upload.docx';
    $tmp = (string) ($file['tmp_name'] ?? '');
    if ($tmp === '' || !is_file($tmp)) {
        docling_job_fail($jobId, 'Uploaded file is invalid.', ['success' => false, 'error' => 'Uploaded file is invalid.']);
        docgen_start_fail(400, 'Uploaded file is invalid.', ['job_id' => $jobId, 'code' => 400]);
    }
    if (!@move_uploaded_file($tmp, $stagedPath) && !@copy($tmp, $stagedPath)) {
        docling_job_fail($jobId, 'Failed to stage uploaded file.', ['success' => false, 'error' => 'Failed to stage uploaded file.']);
        docgen_start_fail(500, 'Failed to stage uploaded file.', ['job_id' => $jobId, 'code' => 500]);
    }

    $requestPayload['notes_file_path'] = $stagedPath;
    $requestPayload['notes_file_name'] = $originalName;
    $requestPayload['source'] = 'docx';
}

$requestPath = $jobDir . '/document_generation_request.json';
if (@file_put_contents($requestPath, json_encode($requestPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)) === false) {
    docling_job_fail($jobId, 'Failed to stage worker request.', ['success' => false, 'error' => 'Failed to stage worker request.']);
    docgen_start_fail(500, 'Failed to stage worker request.', ['job_id' => $jobId, 'code' => 500]);
}

$pid = docgen_start_background_job(DOCGEN_START_WORKER, [
    'job' => $jobId,
    'request_json_path' => $requestPath,
    'user_id' => $userId > 0 ? $userId : null,
], $jobId);
if ($pid === null) {
    docling_job_fail($jobId, 'Failed to start document generation worker.', ['success' => false, 'error' => 'Failed to start document generation worker.']);
    docgen_start_fail(500, 'Failed to start document generation worker.', ['job_id' => $jobId, 'code' => 500]);
}

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 1,
    'current_action' => 'worker spawned',
]);

docling_json_out(202, [
    'success' => true,
    'job_id' => $jobId,
    'pid' => $pid,
    'started' => true,
]);
