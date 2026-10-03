<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

const POLICY_DESIGNATIONS_SCRIPT = '/opt/scraper/scripts/policy_designations_lookup.js';

function policy_designations_api_auth(): void
{
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) {
        docling_json_out(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
    }
}

function policy_designations_fail(int $status, string $message, array $extra = []): void
{
    docling_json_out($status, ['success' => false, 'error' => $message] + $extra);
}

function policy_designations_collect_request(): array
{
    if ($_SERVER['REQUEST_METHOD'] === 'GET') {
        return $_GET;
    }
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        return $_POST;
    }
    policy_designations_fail(405, 'Only GET and POST are supported.', ['code' => 405]);
}

function policy_designations_build_args(array $input): array
{
    $mapping = [
        'application-id' => 'application-id',
        'reference' => 'reference',
        'ons-code' => 'ons-code',
        'postcode' => 'postcode',
        'address' => 'address',
        'lon' => 'lon',
        'lat' => 'lat',
        'easting' => 'easting',
        'northing' => 'northing',
        'limit-per-layer' => 'limit-per-layer',
        'timeout-ms' => 'timeout-ms',
        'output-mode' => 'output-mode',
        'planning-data-limit' => 'planning-data-limit',
        'llm-listed-buildings-limit' => 'llm-listed-buildings-limit',
        'llm-scheduled-monuments-limit' => 'llm-scheduled-monuments-limit',
        'llm-conservation-areas-limit' => 'llm-conservation-areas-limit',
        'nearby-heritage-radius-m' => 'nearby-heritage-radius-m',
        'nearby-heritage-limit' => 'nearby-heritage-limit',
    ];
    $arrayKeys = [
        'planning-data-dataset',
        'planning-data-exclude-dataset',
        'planning-data-exclude-prefix',
        'nearby-heritage-dataset',
    ];
    $boolKeys = [
        'include-planning-data',
        'include-nearby-heritage',
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

    foreach ($arrayKeys as $key) {
        $value = $input[$key] ?? null;
        if ($value === null || $value === '') {
            continue;
        }
        $items = is_array($value) ? $value : [$value];
        foreach ($items as $item) {
            $item = trim((string) $item);
            if ($item === '') {
                continue;
            }
            $args[] = '--' . $key . '=' . $item;
        }
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

function policy_designations_run(array $args): array
{
    $phpBin = '/usr/bin/env';
    $cmd = array_merge([$phpBin, 'node', POLICY_DESIGNATIONS_SCRIPT], $args);

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
        policy_designations_fail(500, 'Failed to start policy designations lookup process.', ['code' => 500]);
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $exit = proc_close($proc);

    $decoded = json_decode((string) $stdout, true);
    if ($exit !== 0) {
        policy_designations_fail(502, 'Lookup process failed.', [
            'code' => 502,
            'details' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }
    if (!is_array($decoded)) {
        policy_designations_fail(502, 'Lookup process returned invalid JSON.', [
            'code' => 502,
            'stderr' => trim((string) $stderr),
            'stdout' => mb_substr((string) $stdout, 0, 500),
        ]);
    }
    return $decoded;
}

policy_designations_api_auth();
$request = policy_designations_collect_request();
$args = policy_designations_build_args($request);
$result = policy_designations_run($args);
docling_json_out(200, $result);
