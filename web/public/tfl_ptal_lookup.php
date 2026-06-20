<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_PTAL_SCRIPT = '/opt/scraper/scripts/tfl_ptal_lookup.js';

function policy_ptal_api_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function policy_ptal_fail(int $status, string $message, array $extra = []): void
{
    docling_json_out($status, ['success' => false, 'error' => $message] + $extra);
}

function policy_ptal_collect_request(): array
{
    if ($_SERVER['REQUEST_METHOD'] === 'GET') {
        return $_GET;
    }
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        return $_POST;
    }
    policy_ptal_fail(405, 'Only GET and POST are supported.', ['code' => 405]);
}

function policy_ptal_build_args(array $input): array
{
    $mapping = [
        'postcode' => 'postcode',
        'address' => 'address',
        'lon' => 'lon',
        'lat' => 'lat',
        'easting' => 'easting',
        'northing' => 'northing',
        'grid-id' => 'grid-id',
        'services-limit' => 'services-limit',
        'timeout-ms' => 'timeout-ms',
    ];
    $boolKeys = [
        'include-services',
        'with-geometry',
    ];

    $args = [];
    foreach ($mapping as $requestKey => $argKey) {
        $value = $input[$requestKey] ?? null;
        if ($value === null || $value === '') {
            continue;
        }
        $args[] = '--' . $argKey . '=' . (string) $value;
    }

    foreach ($boolKeys as $key) {
        if (!array_key_exists($key, $input)) {
            continue;
        }
        $value = strtolower(trim((string) $input[$key]));
        if (in_array($value, ['0', 'false', 'no', 'off'], true)) {
            $args[] = '--no-' . $key;
            continue;
        }
        if (in_array($value, ['1', 'true', 'yes', 'on'], true)) {
            $args[] = '--' . $key;
        }
    }

    $args[] = '--json';
    return $args;
}

function policy_ptal_run(array $args): array
{
    $phpBin = '/usr/bin/env';
    $cmd = array_merge([$phpBin, 'node', POLICY_PTAL_SCRIPT], $args);

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
    $proc = proc_open($command, $descriptors, $pipes, '/opt/scraper');
    if (!is_resource($proc)) {
        policy_ptal_fail(500, 'Failed to start PTAL lookup process.', ['code' => 500]);
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $exit = proc_close($proc);

    $decoded = json_decode((string) $stdout, true);
    if ($exit !== 0) {
        policy_ptal_fail(502, 'PTAL lookup process failed.', [
            'code' => 502,
            'details' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }
    if (!is_array($decoded)) {
        policy_ptal_fail(502, 'PTAL lookup process returned invalid JSON.', [
            'code' => 502,
            'stderr' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }
    return $decoded;
}

policy_ptal_api_auth();
$request = policy_ptal_collect_request();
$args = policy_ptal_build_args($request);
$result = policy_ptal_run($args);
docling_json_out(200, $result);
