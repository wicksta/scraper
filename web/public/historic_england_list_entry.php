<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const HISTORIC_ENGLAND_LIST_ENTRY_SCRIPT = '/opt/scraper/scripts/historic_england_list_entry_extract.js';
const HISTORIC_ENGLAND_LIST_ENTRY_CACHE_DIR = '/tmp/historic_england_list_entry_cache';
const HISTORIC_ENGLAND_LIST_ENTRY_CACHE_TTL = 604800;
const HISTORIC_ENGLAND_PLAYWRIGHT_BROWSERS_PATH = '/tmp/historic_england_ms_playwright';

function he_list_entry_api_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function he_list_entry_fail(int $status, string $message, array $extra = []): void
{
    docling_json_out($status, ['success' => false, 'error' => $message] + $extra);
}

function he_list_entry_collect_request(): array
{
    if ($_SERVER['REQUEST_METHOD'] === 'GET') {
        return $_GET;
    }
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        return $_POST;
    }
    he_list_entry_fail(405, 'Only GET and POST are supported.', ['code' => 405]);
}

function he_list_entry_cache_path(string $ref, string $section): string
{
    $safeSection = preg_replace('/[^a-z0-9_-]+/i', '_', $section) ?: 'official-list-entry';
    if (!is_dir(HISTORIC_ENGLAND_LIST_ENTRY_CACHE_DIR)) {
        @mkdir(HISTORIC_ENGLAND_LIST_ENTRY_CACHE_DIR, 0775, true);
    }
    return HISTORIC_ENGLAND_LIST_ENTRY_CACHE_DIR . '/' . $ref . '__' . $safeSection . '.json';
}

function he_list_entry_cache_get(string $ref, string $section): ?array
{
    $path = he_list_entry_cache_path($ref, $section);
    if (!is_file($path)) {
        return null;
    }
    $mtime = @filemtime($path);
    if (!$mtime || ($mtime + HISTORIC_ENGLAND_LIST_ENTRY_CACHE_TTL) < time()) {
        return null;
    }
    $raw = @file_get_contents($path);
    if ($raw === false || trim($raw) === '') {
        return null;
    }
    $decoded = json_decode($raw, true);
    return is_array($decoded) ? $decoded : null;
}

function he_list_entry_cache_put(string $ref, string $section, array $payload): void
{
    $path = he_list_entry_cache_path($ref, $section);
    @file_put_contents($path, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT));
}

function he_list_entry_run(string $ref, string $section, int $timeoutMs): array
{
    $cmd = [
        '/usr/bin/env',
        'node',
        HISTORIC_ENGLAND_LIST_ENTRY_SCRIPT,
        '--ref=' . $ref,
        '--section=' . $section,
        '--timeout-ms=' . (string) $timeoutMs,
    ];

    $escaped = [];
    foreach ($cmd as $part) {
        $escaped[] = escapeshellarg($part);
    }
    $command = implode(' ', $escaped);

    $descriptors = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];
    $env = $_ENV;
    $env['PLAYWRIGHT_BROWSERS_PATH'] = HISTORIC_ENGLAND_PLAYWRIGHT_BROWSERS_PATH;
    $proc = proc_open($command, $descriptors, $pipes, '/opt/scraper', $env);
    if (!is_resource($proc)) {
        he_list_entry_fail(500, 'Failed to start Historic England extraction process.', ['code' => 500]);
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $exit = proc_close($proc);

    $decoded = json_decode((string) $stdout, true);
    if ($exit !== 0) {
        he_list_entry_fail(502, 'Historic England extraction failed.', [
            'code' => 502,
            'details' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }
    if (!is_array($decoded)) {
        he_list_entry_fail(502, 'Historic England extraction returned invalid JSON.', [
            'code' => 502,
            'stderr' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }

    return [
        'ok' => true,
        'ref' => $ref,
        'canonical_url' => (string) ($decoded['canonical_url'] ?? ''),
        'entry_title' => (string) ($decoded['entry_title'] ?? ''),
        'date_first_listed' => (string) (($decoded['official_list_entry']['key_info']['Date first listed:'] ?? '') ?: ($decoded['overview']['summary']['Date first listed:'] ?? '')),
        'details_text' => (string) ($decoded['official_list_entry']['details_text'] ?? ''),
        'fetched_at_utc' => gmdate('c'),
    ];
}

he_list_entry_api_auth();
$request = he_list_entry_collect_request();
$ref = trim((string) ($request['ref'] ?? ''));
$section = trim((string) ($request['section'] ?? 'official-list-entry')) ?: 'official-list-entry';
$timeoutMs = max(10000, min(120000, (int) ($request['timeout-ms'] ?? 45000)));

if (!preg_match('/^\d+$/', $ref)) {
    he_list_entry_fail(400, 'A numeric Historic England list entry ref is required.', ['code' => 400]);
}

$cached = he_list_entry_cache_get($ref, $section);
if (is_array($cached)) {
    $cached['cache_hit'] = true;
    docling_json_out(200, $cached);
}

$result = he_list_entry_run($ref, $section, $timeoutMs);
$result['cache_hit'] = false;
he_list_entry_cache_put($ref, $section, $result);
docling_json_out(200, $result);
