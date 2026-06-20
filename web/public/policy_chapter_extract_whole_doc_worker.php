#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_CHAPTER_WHOLE_DOC_NODE = '/usr/bin/node';
const POLICY_CHAPTER_WHOLE_DOC_SCRIPT = '/opt/scraper/policy/extract_policy_chapters_whole_doc.js';
const POLICY_CHAPTER_WHOLE_DOC_CHECKPOINT_BASE_DIR = '/tmp/policy-chapter-extraction';

function policy_chapter_whole_doc_arg(string $name, ?string $default = null): ?string
{
    global $argv;
    foreach (array_slice($argv, 1) as $arg) {
        if (!str_starts_with($arg, '--')) continue;
        $pos = strpos($arg, '=');
        if ($pos === false) continue;
        $key = substr($arg, 2, $pos - 2);
        if ($key === $name) {
            return substr($arg, $pos + 1);
        }
    }
    return $default;
}

function policy_chapter_whole_doc_fail(int $jobId, string $message, array $payload = [], int $code = 1): never
{
    $final = ['success' => false, 'error' => $message] + $payload;
    docling_worker_log($jobId, 'ERROR ' . $message);
    docling_job_fail($jobId, $message, $final);
    exit($code);
}

function policy_chapter_whole_doc_progress_from_event(array $event, string $mode): array
{
    $name = (string) ($event['event'] ?? '');
    $percent = null;
    $action = null;

    switch ($name) {
        case 'extractor.start':
            $percent = 2;
            $action = $mode === 'execute' ? 'Starting whole-document chapter extraction' : 'Starting whole-document chapter preview';
            break;
        case 'document.loaded':
            $percent = 8;
            $action = 'Loaded policy document';
            break;
        case 'pages.ready':
            $percent = 16;
            $action = sprintf('Prepared %d pages for whole-document review', (int) ($event['pages'] ?? 0));
            break;
        case 'whole_doc.start':
            $percent = 28;
            $action = 'Sending full document to GPT 5.4';
            break;
        case 'merge.completed':
            $percent = 84;
            $action = sprintf('Built %d top-level chapters', (int) ($event['chapters_final'] ?? 0));
            break;
        case 'store.begin':
            $percent = 92;
            $action = 'Writing policy chapters to Postgres';
            break;
        case 'store.completed':
            $percent = 98;
            $action = sprintf('Stored %d policy chapters', (int) ($event['chapters'] ?? 0));
            break;
        case 'extractor.completed':
            $percent = 100;
            $action = $mode === 'execute' ? 'Whole-document policy chapter extraction completed' : 'Whole-document policy chapter preview completed';
            break;
    }

    return [$percent, $action];
}

function policy_chapter_whole_doc_build_command(array $request): array
{
    $node = is_executable(POLICY_CHAPTER_WHOLE_DOC_NODE) ? POLICY_CHAPTER_WHOLE_DOC_NODE : 'node';
    $parts = [
        escapeshellarg($node),
        escapeshellarg(POLICY_CHAPTER_WHOLE_DOC_SCRIPT),
        '--doc-id=' . escapeshellarg((string) $request['doc_id']),
        '--model=' . escapeshellarg((string) ($request['model'] ?? 'gpt-5.4')),
        !empty($request['apply']) ? '--apply=true' : '--apply=false',
    ];
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

$jobId = (int) policy_chapter_whole_doc_arg('job', '0');
$requestJsonPath = (string) policy_chapter_whole_doc_arg('request-json-path', '');
if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job\n");
    exit(2);
}
if ($requestJsonPath === '' || !is_file($requestJsonPath)) {
    policy_chapter_whole_doc_fail($jobId, 'Missing request JSON for whole-document policy chapter extraction.');
}

$request = json_decode((string) file_get_contents($requestJsonPath), true);
if (!is_array($request)) {
    policy_chapter_whole_doc_fail($jobId, 'Invalid request JSON for whole-document policy chapter extraction.');
}

$docId = trim((string) ($request['doc_id'] ?? ''));
$mode = trim((string) ($request['mode'] ?? 'test'));
if ($docId === '') {
    policy_chapter_whole_doc_fail($jobId, 'Missing doc_id for whole-document policy chapter extraction.');
}
if (!in_array($mode, ['test', 'execute'], true)) {
    policy_chapter_whole_doc_fail($jobId, 'Invalid whole-document policy chapter extraction mode.');
}

$checkpointDir = POLICY_CHAPTER_WHOLE_DOC_CHECKPOINT_BASE_DIR . '/job_' . $jobId;
if (!is_dir($checkpointDir)) {
    @mkdir($checkpointDir, 0777, true);
}
$checkpointPath = $checkpointDir . '/extract_whole_doc_checkpoint.json';
$request['checkpoint_path'] = $checkpointPath;
@file_put_contents($requestJsonPath, json_encode($request, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

docling_job_update($jobId, [
    'status' => 'running',
    'percentage_complete' => 1,
    'current_action' => $mode === 'execute' ? 'Queued whole-document chapter extraction' : 'Queued whole-document chapter preview',
]);

$cmdInfo = policy_chapter_whole_doc_build_command($request);
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
    policy_chapter_whole_doc_fail($jobId, 'Failed to launch whole-document policy chapter extraction process.');
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
    if (is_resource($pipes[1])) $read[] = $pipes[1];
    if (is_resource($pipes[2])) $read[] = $pipes[2];

    if ($read) {
        $write = null;
        $except = null;
        @stream_select($read, $write, $except, $running ? 1 : 0, 200000);
        foreach ($read as $stream) {
            $chunk = stream_get_contents($stream);
            if ($chunk === false || $chunk === '') continue;
            if ($stream === $pipes[1]) {
                $stdout .= $chunk;
                continue;
            }
            foreach (preg_split("/\r\n|\n|\r/", $chunk) ?: [] as $line) {
                $line = trim($line);
                if ($line === '') continue;
                $stderrTail[] = $line;
                if (count($stderrTail) > 100) array_shift($stderrTail);
                docling_worker_log($jobId, '[policy-chapter-whole-doc] ' . $line);
                $event = json_decode($line, true);
                if (is_array($event)) {
                    [$percent, $action] = policy_chapter_whole_doc_progress_from_event($event, $mode);
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
    if ($line === '') continue;
    $stderrTail[] = $line;
    if (count($stderrTail) > 100) array_shift($stderrTail);
    docling_worker_log($jobId, '[policy-chapter-whole-doc] ' . $line);
}
fclose($pipes[1]);
fclose($pipes[2]);
$exitCode = proc_close($proc);

$decoded = json_decode(trim($stdout), true);
if ($exitCode !== 0) {
    policy_chapter_whole_doc_fail($jobId, 'Whole-document policy chapter extraction process failed.', [
        'doc_id' => $docId,
        'mode' => $mode,
        'exit_code' => $exitCode,
        'stderr_tail' => array_slice($stderrTail, -20),
        'stdout_tail' => mb_substr(trim($stdout), -2000),
    ]);
}
if (!is_array($decoded) || empty($decoded['success'])) {
    policy_chapter_whole_doc_fail($jobId, 'Whole-document policy chapter extraction returned invalid output.', [
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
    'result' => $decoded,
];
docling_job_finish($jobId, $finalOutput, 'completed');
docling_worker_log($jobId, 'Completed whole-document policy chapter extraction job.');
