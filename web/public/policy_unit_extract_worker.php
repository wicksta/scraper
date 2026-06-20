#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_UNIT_NODE = '/usr/bin/node';
const POLICY_UNIT_SCRIPT = '/opt/scraper/policy/extract_policy_units.js';
const POLICY_UNIT_CHECKPOINT_BASE_DIR = '/tmp/policy-unit-extraction';

function policy_unit_arg(string $name, ?string $default = null): ?string
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

function policy_unit_pg_pdo(): PDO
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

function policy_unit_fail(int $jobId, string $message, array $payload = [], int $code = 1): never
{
    $final = ['success' => false, 'error' => $message] + $payload;
    docling_worker_log($jobId, 'ERROR ' . $message);
    docling_job_fail($jobId, $message, $final);
    exit($code);
}

function policy_unit_update_guidance_job(PDO $pg, string $docId, string $mode, int $jobId): void
{
    $column = $mode === 'execute' ? 'last_execute_job_id' : 'last_test_job_id';
    $sql = sprintf(
        'UPDATE public.policy_document_guidance SET %s = :job_id, updated_at = now() WHERE doc_id = :doc_id::uuid',
        $column
    );
    // Postgres does not allow bind placeholders inside ::uuid syntax, so use CAST.
    $sql = sprintf(
        'UPDATE public.policy_document_guidance SET %s = :job_id, updated_at = now() WHERE doc_id = CAST(:doc_id AS uuid)',
        $column
    );
    $st = $pg->prepare($sql);
    $st->execute([
        ':job_id' => $jobId,
        ':doc_id' => $docId,
    ]);
}

function policy_unit_progress_from_event(array $event, string $mode): array
{
    $name = (string) ($event['event'] ?? '');
    $percent = null;
    $action = null;

    switch ($name) {
        case 'extractor.start':
            $percent = 2;
            $action = $mode === 'execute' ? 'Starting full policy analysis' : 'Starting policy prompt test';
            break;
        case 'document.loaded':
            $percent = 8;
            $action = 'Loaded policy document and guidance';
            break;
        case 'windows.built':
            $percent = 12;
            $pageStart = $event['page_start'] ?? null;
            $pageEnd = $event['page_end'] ?? null;
            $suffix = ($pageStart && $pageEnd) ? sprintf(' for pages %s-%s', $pageStart, $pageEnd) : '';
            $action = sprintf('Built %d extraction windows%s', (int) ($event['windows'] ?? 0), $suffix);
            break;
        case 'window.start':
            $windowNo = max(1, (int) ($event['window_no'] ?? 1));
            $windowCount = max($windowNo, (int) ($event['window_count'] ?? $windowNo));
            $ratio = $windowCount > 0 ? ($windowNo - 1) / $windowCount : 0;
            $percent = 15 + (int) floor($ratio * 55);
            $action = sprintf(
                'Analysing pages %d-%d (%d/%d)',
                (int) ($event['page_start'] ?? 0),
                (int) ($event['page_end'] ?? 0),
                $windowNo,
                $windowCount
            );
            break;
        case 'window.completed':
            $windowNo = max(1, (int) ($event['window_no'] ?? 1));
            $windowCount = max($windowNo, (int) ($event['window_count'] ?? $windowNo));
            $ratio = $windowCount > 0 ? $windowNo / $windowCount : 1;
            $percent = 15 + (int) floor($ratio * 55);
            $action = sprintf(
                'Completed pages %d-%d (%d/%d)',
                (int) ($event['page_start'] ?? 0),
                (int) ($event['page_end'] ?? 0),
                $windowNo,
                $windowCount
            );
            break;
        case 'merge.completed':
            $percent = 75;
            $action = sprintf('Merged %d extracted policy units', (int) ($event['units_final'] ?? 0));
            break;
        case 'embeddings.batch.start':
            $batchNo = max(1, (int) ($event['batch_no'] ?? 1));
            $batchCount = max($batchNo, (int) ($event['batch_count'] ?? $batchNo));
            $ratio = $batchCount > 0 ? ($batchNo - 1) / $batchCount : 0;
            $percent = 78 + (int) floor($ratio * 12);
            $action = sprintf('Embedding units (%d/%d)', $batchNo, $batchCount);
            break;
        case 'embeddings.batch.completed':
            $batchNo = max(1, (int) ($event['batch_no'] ?? 1));
            $batchCount = max($batchNo, (int) ($event['batch_count'] ?? $batchNo));
            $ratio = $batchCount > 0 ? $batchNo / $batchCount : 1;
            $percent = 78 + (int) floor($ratio * 12);
            $action = sprintf('Embedded units (%d/%d)', $batchNo, $batchCount);
            break;
        case 'store.begin':
            $percent = 92;
            $action = 'Writing policy units to Postgres';
            break;
        case 'store.completed':
            $percent = 98;
            $action = sprintf('Stored %d policy units', (int) ($event['units'] ?? 0));
            break;
        case 'extractor.completed':
            $percent = 100;
            $action = $mode === 'execute' ? 'Full policy analysis completed' : 'Policy prompt test completed';
            break;
    }

    return [$percent, $action];
}

function policy_unit_build_command(array $request): array
{
    $node = is_executable(POLICY_UNIT_NODE) ? POLICY_UNIT_NODE : 'node';
    $parts = [
        escapeshellarg($node),
        escapeshellarg(POLICY_UNIT_SCRIPT),
        '--doc-id=' . escapeshellarg((string) $request['doc_id']),
        '--model=' . escapeshellarg((string) ($request['model'] ?? 'gpt-5-mini')),
    ];

    if (!empty($request['apply'])) {
        $parts[] = '--apply=true';
        $parts[] = '--embed=true';
    } else {
        $parts[] = '--apply=false';
        $parts[] = '--embed=false';
    }

    if (!empty($request['page_start'])) {
        $parts[] = '--page-start=' . escapeshellarg((string) $request['page_start']);
    }
    if (!empty($request['page_end'])) {
        $parts[] = '--page-end=' . escapeshellarg((string) $request['page_end']);
    }
    if (!empty($request['window_pages'])) {
        $parts[] = '--window-pages=' . escapeshellarg((string) $request['window_pages']);
    }
    if (!empty($request['overlap_pages'])) {
        $parts[] = '--overlap-pages=' . escapeshellarg((string) $request['overlap_pages']);
    }
    if (!empty($request['limit_windows'])) {
        $parts[] = '--limit-windows=' . escapeshellarg((string) $request['limit_windows']);
    }
    if (!empty($request['checkpoint_path'])) {
        $parts[] = '--checkpoint-path=' . escapeshellarg((string) $request['checkpoint_path']);
    }

    return [
        'command' => implode(' ', $parts),
        'node' => $node,
    ];
}

if (PHP_SAPI !== 'cli') {
    http_response_code(403);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['success' => false, 'error' => 'CLI only.']);
    exit;
}

$jobId = (int) policy_unit_arg('job', '0');
$requestJsonPath = (string) policy_unit_arg('request-json-path', '');
if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job\n");
    exit(2);
}
if ($requestJsonPath === '' || !is_file($requestJsonPath)) {
    policy_unit_fail($jobId, 'Missing request JSON for policy unit extraction.');
}

$request = json_decode((string) file_get_contents($requestJsonPath), true);
if (!is_array($request)) {
    policy_unit_fail($jobId, 'Invalid request JSON for policy unit extraction.');
}

$docId = trim((string) ($request['doc_id'] ?? ''));
$mode = trim((string) ($request['mode'] ?? 'test'));
$userId = max(0, (int) ($request['user_id'] ?? 0));
if ($docId === '') {
    policy_unit_fail($jobId, 'Missing doc_id for policy unit extraction.');
}
if (!in_array($mode, ['test', 'execute'], true)) {
    policy_unit_fail($jobId, 'Invalid policy unit extraction mode.');
}

$jobDir = docling_job_dir($jobId);
$checkpointDir = POLICY_UNIT_CHECKPOINT_BASE_DIR . '/job_' . $jobId;
if (!is_dir($checkpointDir)) {
    @mkdir($checkpointDir, 0777, true);
}
$checkpointPath = $checkpointDir . '/extract_checkpoint.json';
$request['checkpoint_path'] = $checkpointPath;
@file_put_contents($requestJsonPath, json_encode($request, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 1,
    'current_action' => $mode === 'execute' ? 'Queued full policy analysis' : 'Queued policy prompt test',
]);

try {
    $pg = policy_unit_pg_pdo();
    policy_unit_update_guidance_job($pg, $docId, $mode, $jobId);
} catch (Throwable $e) {
    docling_worker_log($jobId, 'Failed to update policy_document_guidance job pointer: ' . $e->getMessage());
}

$cmdInfo = policy_unit_build_command($request);
docling_worker_log($jobId, 'Launching command: ' . $cmdInfo['command']);

$env = array_merge($_ENV, [
    'HOME' => '/home/james',
    'PYTHONUNBUFFERED' => '1',
]);

$proc = @proc_open(
    $cmdInfo['command'],
    [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ],
    $pipes,
    '/opt/scraper',
    $env
);

if (!is_resource($proc)) {
    policy_unit_fail($jobId, 'Failed to launch policy unit extraction process.');
}

fclose($pipes[0]);
stream_set_blocking($pipes[1], false);
stream_set_blocking($pipes[2], false);

$stdout = '';
$stderrTail = [];

while (true) {
    $status = proc_get_status($proc);
    $running = (bool) ($status['running'] ?? false);
    $read = [];
    if (is_resource($pipes[1])) {
        $read[] = $pipes[1];
    }
    if (is_resource($pipes[2])) {
        $read[] = $pipes[2];
    }

    if ($read) {
        $write = null;
        $except = null;
        @stream_select($read, $write, $except, $running ? 1 : 0, 200000);
        foreach ($read as $stream) {
            $chunk = stream_get_contents($stream);
            if ($chunk === false || $chunk === '') {
                continue;
            }
            if ($stream === $pipes[1]) {
                $stdout .= $chunk;
                continue;
            }
            foreach (preg_split("/\r\n|\n|\r/", $chunk) ?: [] as $line) {
                $line = trim($line);
                if ($line === '') {
                    continue;
                }
                $stderrTail[] = $line;
                if (count($stderrTail) > 100) {
                    array_shift($stderrTail);
                }
                docling_worker_log($jobId, '[policy-unit] ' . $line);
                $event = json_decode($line, true);
                if (is_array($event)) {
                    [$percent, $action] = policy_unit_progress_from_event($event, $mode);
                    if ($percent !== null || $action !== null) {
                        docling_job_update($jobId, array_filter([
                            'percentage_complete' => $percent,
                            'current_action' => $action,
                        ], static fn($v) => $v !== null));
                    }
                }
            }
        }
    }

    if (!$running) {
        break;
    }
}

$stdout .= stream_get_contents($pipes[1]) ?: '';
$stderrRemainder = stream_get_contents($pipes[2]) ?: '';
foreach (preg_split("/\r\n|\n|\r/", $stderrRemainder) ?: [] as $line) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }
    $stderrTail[] = $line;
    if (count($stderrTail) > 100) {
        array_shift($stderrTail);
    }
    docling_worker_log($jobId, '[policy-unit] ' . $line);
}
fclose($pipes[1]);
fclose($pipes[2]);
$exitCode = proc_close($proc);

$decoded = json_decode(trim($stdout), true);
if ($exitCode !== 0) {
    policy_unit_fail($jobId, 'Policy unit extraction process failed.', [
        'doc_id' => $docId,
        'mode' => $mode,
        'exit_code' => $exitCode,
        'stderr_tail' => array_slice($stderrTail, -20),
        'stdout_tail' => mb_substr(trim($stdout), -2000),
    ]);
}
if (!is_array($decoded) || empty($decoded['success'])) {
    policy_unit_fail($jobId, 'Policy unit extraction returned invalid output.', [
        'doc_id' => $docId,
        'mode' => $mode,
        'stderr_tail' => array_slice($stderrTail, -20),
        'stdout_tail' => mb_substr(trim($stdout), -2000),
    ]);
}

$finalOutput = [
    'success' => true,
    'doc_id' => $docId,
    'mode' => $mode,
    'job_id' => $jobId,
    'page_start' => $request['page_start'] ?? null,
    'page_end' => $request['page_end'] ?? null,
    'result' => $decoded,
];
docling_job_finish($jobId, $finalOutput, 'completed');
docling_worker_log($jobId, 'Completed policy unit extraction job.');
