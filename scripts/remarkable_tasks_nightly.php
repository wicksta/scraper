#!/usr/bin/env php
<?php
declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Wrong SAPI.
");
    exit(2);
}

require_once '/var/www/html/docling_job_lib.php';

const NIGHTLY_USER_ID = 10;
const NIGHTLY_DOCUMENT = 'To Do: Today';
const NIGHTLY_MODEL = 'gpt-5.6-luna';
const NIGHTLY_APPLY_MODE = 'confident';
const NIGHTLY_INTERPRET_WORKER = '/var/www/html/remarkable_task_interpret_worker.php';
const NIGHTLY_EXPORT_WORKER = '/var/www/html/remarkable_task_export_worker.php';
const NIGHTLY_LOCK_FILE = '/tmp/ngist-remarkable-tasks-nightly.lock';

function nightly_log(string $message): void
{
    fwrite(STDOUT, '[' . gmdate('c') . '] ' . $message . PHP_EOL);
}

function nightly_php_bin(): string
{
    foreach (['/usr/local/php82/bin/php-cli', '/usr/bin/php', PHP_BINARY] as $candidate) {
        if (is_string($candidate) && $candidate !== '' && is_executable($candidate)) {
            return $candidate;
        }
    }
    return 'php';
}

function nightly_create_job(string $kind, string $action): int
{
    return docling_job_create([
        'kind' => $kind,
        'user_id' => NIGHTLY_USER_ID,
        'stage' => 1,
        'status' => 'queued',
        'percentage_complete' => 0,
        'current_action' => $action,
    ]);
}

function nightly_run_worker(string $worker, array $args): void
{
    if (!is_file($worker)) {
        throw new RuntimeException('Worker is missing: ' . $worker);
    }

    $parts = [escapeshellarg(nightly_php_bin()), escapeshellarg($worker)];
    foreach ($args as $key => $value) {
        $flag = '--' . preg_replace('~[^a-z0-9_-]~i', '', (string)$key);
        $parts[] = $flag . '=' . escapeshellarg((string)$value);
    }

    $cmd = implode(' ', $parts);
    $output = [];
    $code = 0;
    exec($cmd . ' 2>&1', $output, $code);
    foreach ($output as $line) {
        if (trim((string)$line) !== '') {
            nightly_log('worker: ' . (string)$line);
        }
    }
    if ($code !== 0) {
        throw new RuntimeException('Worker exited with code ' . $code . ': ' . mb_substr(trim(implode("
", $output)), 0, 1200));
    }
}

function nightly_completed_output(int $jobId, string $label): array
{
    $job = docling_job_get($jobId);
    if (!$job) {
        throw new RuntimeException($label . ' job disappeared: #' . $jobId);
    }
    $status = strtolower((string)($job['status'] ?? ''));
    $output = is_array($job['final_output'] ?? null) ? $job['final_output'] : [];
    if ($status !== 'completed' || (($output['success'] ?? false) !== true)) {
        $error = (string)($job['error_message'] ?? $output['error'] ?? $label . ' job did not complete.');
        throw new RuntimeException($label . ' job #' . $jobId . ' failed: ' . $error);
    }
    return $output;
}

$lock = fopen(NIGHTLY_LOCK_FILE, 'c');
if (!$lock) {
    fwrite(STDERR, 'Could not open lock file: ' . NIGHTLY_LOCK_FILE . PHP_EOL);
    exit(1);
}
if (!flock($lock, LOCK_EX | LOCK_NB)) {
    nightly_log('Another nightly reMarkable task loop is already running; exiting.');
    exit(0);
}

try {
    docling_cleanup_old_job_dirs();

    $interpretJobId = nightly_create_job('beta/personal_tasks_interpret_remarkable', 'queued nightly reMarkable task interpretation');
    nightly_log('Starting interpretation job #' . $interpretJobId . ' for ' . NIGHTLY_DOCUMENT . '.');
    nightly_run_worker(NIGHTLY_INTERPRET_WORKER, [
        'job' => $interpretJobId,
        'user_id' => NIGHTLY_USER_ID,
        'document' => NIGHTLY_DOCUMENT,
        'model' => NIGHTLY_MODEL,
        'apply_mode' => NIGHTLY_APPLY_MODE,
    ]);
    $interpret = nightly_completed_output($interpretJobId, 'Interpretation');
    $counts = is_array($interpret['counts'] ?? null) ? $interpret['counts'] : [];
    nightly_log(sprintf(
        'Interpretation complete: closed %d, created %d, people updates %d, proposed %d, skipped %d.',
        (int)($counts['completed'] ?? 0),
        (int)($counts['created'] ?? 0),
        (int)($counts['people_updated'] ?? 0),
        (int)($counts['proposed'] ?? 0),
        (int)($counts['skipped'] ?? 0)
    ));

    $exportJobId = nightly_create_job('beta/personal_tasks_export_remarkable', 'queued nightly reMarkable task export');
    nightly_log('Starting export job #' . $exportJobId . '.');
    nightly_run_worker(NIGHTLY_EXPORT_WORKER, [
        'job' => $exportJobId,
        'user_id' => NIGHTLY_USER_ID,
    ]);
    $export = nightly_completed_output($exportJobId, 'Export');
    nightly_log('Export complete: uploaded ' . (string)($export['remarkable_document'] ?? 'To Do: Today') . ' to ' . (string)($export['remarkable_folder'] ?? 'root') . '.');

    $notesBatchJobId = nightly_create_job('beta/personal_notes_batch_remarkable', "queued nightly Today's Notes batch");
    nightly_log("Starting Today's Notes batch job #" . $notesBatchJobId . '.');
    nightly_run_worker(NIGHTLY_EXPORT_WORKER, [
        'job' => $notesBatchJobId,
        'user_id' => NIGHTLY_USER_ID,
        'batch_todays_notes' => '1',
    ]);
    $notesBatch = nightly_completed_output($notesBatchJobId, "Today's Notes batch");
    nightly_log('Today\'s Notes batch: ' . (string)($notesBatch['mode'] ?? 'completed') . '.');
    nightly_log('Nightly reMarkable task loop completed.');
    exit(0);
} catch (Throwable $e) {
    nightly_log('Nightly reMarkable task loop failed: ' . $e->getMessage());
    exit(1);
} finally {
    flock($lock, LOCK_UN);
    fclose($lock);
}
