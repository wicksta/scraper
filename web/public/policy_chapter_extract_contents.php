<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_CHAPTER_CONTENTS_WORKER_SCRIPT = '/var/www/html/policy_chapter_extract_contents_worker.php';

docling_cleanup_old_job_dirs();

function policy_chapter_contents_api_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function policy_chapter_contents_start_background_job(string $workerPhp, array $args, int $jobId): ?string
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
            @file_put_contents($logFile, '[' . gmdate('c') . '] at handoff failed: ' . $joined . PHP_EOL, FILE_APPEND);
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

policy_chapter_contents_api_auth();

if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    $jobId = (int) ($_GET['job_id'] ?? $_GET['jobId'] ?? 0);
    if ($jobId <= 0) {
        docling_json_out(400, ['success' => false, 'error' => 'missing or invalid job_id', 'code' => 400]);
    }
    $job = docling_job_get($jobId);
    if (!$job) {
        docling_json_out(404, ['success' => false, 'error' => 'job not found', 'code' => 404]);
    }
    docling_json_out(200, ['success' => true, 'job' => $job]);
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    docling_json_out(405, ['success' => false, 'error' => 'Only GET and POST are supported.', 'code' => 405]);
}

$docId = trim((string) ($_POST['doc_id'] ?? ''));
$mode = trim((string) ($_POST['mode'] ?? 'test'));
$userId = max(0, (int) ($_POST['user_id'] ?? 0));

if ($docId === '') {
    docling_json_out(400, ['success' => false, 'error' => 'doc_id is required.', 'code' => 400]);
}
if (!in_array($mode, ['test', 'execute'], true)) {
    docling_json_out(400, ['success' => false, 'error' => 'mode must be test or execute.', 'code' => 400]);
}

$jobFields = [
    'kind' => $mode === 'execute' ? 'policy/extract_chapters_from_contents_execute' : 'policy/extract_chapters_from_contents_test',
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
    'doc_id' => $docId,
    'mode' => $mode,
    'user_id' => $userId > 0 ? $userId : null,
    'model' => 'gpt-5-mini',
    'apply' => $mode === 'execute',
];
$requestJsonPath = $jobDir . '/policy_chapter_contents_request.json';
@file_put_contents($requestJsonPath, json_encode($requestPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

$pid = policy_chapter_contents_start_background_job(POLICY_CHAPTER_CONTENTS_WORKER_SCRIPT, [
    'job' => $jobId,
    'request-json-path' => $requestJsonPath,
], $jobId);

if ($pid === null) {
    docling_job_fail($jobId, 'Failed to start contents-led policy chapter worker.');
    docling_json_out(500, ['success' => false, 'error' => 'Failed to start contents-led policy chapter worker.', 'job_id' => $jobId, 'code' => 500]);
}

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 1,
    'current_action' => $mode === 'execute' ? 'Contents-led chapter extraction queued' : 'Contents-led chapter preview queued',
]);

docling_json_out(202, [
    'success' => true,
    'job_id' => $jobId,
    'pid' => $pid,
    'status' => 'running',
    'mode' => $mode,
    'status_path' => basename(__FILE__) . '?job_id=' . $jobId . '&api_key=' . rawurlencode(docling_request_api_key()),
]);
