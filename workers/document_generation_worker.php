#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once '/var/www/html/docling_job_lib.php';

if (!defined('OPENAI_API_KEY')) {
    define('OPENAI_API_KEY', (string) docling_env('OPENAI_API_KEY', ''));
}

require_once __DIR__ . '/document_generation_library.php';

function docgen_worker_parse_cli(array $argv): array
{
    $args = [];
    foreach (array_slice($argv, 1) as $arg) {
        if (preg_match('~^--([a-z0-9_-]+)=(.*)$~i', $arg, $m)) {
            $args[strtolower($m[1])] = $m[2];
        }
    }

    return [
        'job_id' => isset($args['job']) ? (int) $args['job'] : 0,
        'request_json_path' => trim((string) ($args['request_json_path'] ?? '')),
    ];
}

function docgen_worker_fail(int $jobId, string $message, array $extra = [], int $code = 1): never
{
    $payload = ['success' => false, 'error' => $message] + $extra;
    docling_job_fail($jobId, $message, $payload);
    fwrite(STDERR, $message . PHP_EOL);
    exit($code);
}

function docgen_worker_require_file(string $path, string $message): void
{
    if ($path === '' || !is_file($path) || !is_readable($path)) {
        throw new RuntimeException($message);
    }
}

if (PHP_SAPI !== 'cli') {
    http_response_code(405);
    echo 'CLI only';
    exit(2);
}

$args = docgen_worker_parse_cli($argv);
$jobId = $args['job_id'];
$requestPath = $args['request_json_path'];

if ($jobId <= 0) {
    fwrite(STDERR, "Missing --job.\n");
    exit(2);
}

try {
    docling_job_update($jobId, [
        'status' => 'running',
        'percentage_complete' => 5,
        'current_action' => 'loading request',
    ]);

    docgen_worker_require_file($requestPath, 'Worker request file is missing.');
    $request = json_decode((string) file_get_contents($requestPath), true);
    if (!is_array($request)) {
        throw new RuntimeException('Worker request payload is invalid JSON.');
    }

    $docType = strtolower(trim((string) ($request['doc_type'] ?? '')));
    if (!in_array($docType, ['note', 'letter', 'minutes'], true)) {
        throw new RuntimeException('Invalid or missing doc_type.');
    }

    $notes = trim((string) ($request['notes_text'] ?? ''));
    $source = trim((string) ($request['source'] ?? '')) ?: null;
    $notesFilePath = trim((string) ($request['notes_file_path'] ?? ''));

    if ($notes === '' && $notesFilePath !== '') {
        docling_job_update($jobId, [
            'percentage_complete' => 15,
            'current_action' => 'reading uploaded DOCX',
        ]);
        docgen_worker_require_file($notesFilePath, 'Uploaded DOCX could not be found.');
        $notes = docgen_read_docx_text($notesFilePath);
        $source = 'docx';
    }

    if ($notes === '') {
        throw new RuntimeException('Provide draft notes text or upload a .docx file.');
    }

    docling_job_update($jobId, [
        'percentage_complete' => 35,
        'current_action' => 'sending prompt to OpenAI',
    ]);

    $payload = docgen_generate_structured($docType, $notes, $source);

    docling_job_update($jobId, [
        'percentage_complete' => 95,
        'current_action' => 'saving result',
    ]);

    docling_job_finish($jobId, $payload, 'completed');
    fwrite(STDOUT, json_encode(['job_id' => $jobId, 'success' => true], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(0);
} catch (RuntimeException $e) {
    $prev = $e->getPrevious();
    $extra = [];
    if ($prev instanceof RuntimeException && $prev->getMessage() !== '') {
        $decoded = json_decode($prev->getMessage(), true);
        if (is_array($decoded)) {
            $extra['stage'] = 'openai';
            $extra['openai'] = $decoded;
        }
    }
    docgen_worker_fail($jobId, $e->getMessage(), $extra);
} catch (Throwable $e) {
    docgen_worker_fail($jobId, $e->getMessage());
}
