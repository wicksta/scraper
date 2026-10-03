<?php
declare(strict_types=1);

require_once __DIR__ . '/docling_job_lib.php';

function lvmf_2026_auth(): void {
    $expected = docling_expected_api_key();
    $provided = docling_request_api_key();
    if ($expected === '' || !hash_equals($expected, $provided)) docling_json_out(403, ['success' => false, 'error' => 'Access denied.']);
}
function lvmf_2026_pg(): PDO {
    $host = (string) docling_env('PGHOST', '127.0.0.1');
    $port = (string) docling_env('PGPORT', '5432');
    $database = (string) docling_env('PGDATABASE', 'docs_db');
    $user = (string) docling_env('PGUSER', 'webapp');
    $password = (string) docling_env('PGPASSWORD', '');
    return new PDO("pgsql:host=$host;port=$port;dbname=$database;sslmode=" . docling_env('PGSSLMODE', 'require'), $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC]);
}
function lvmf_2026_json_error(int $status, string $message): never { docling_json_out($status, ['success' => false, 'error' => $message]); }
function lvmf_2026_number(mixed $value, string $name, float $minimum, float $maximum): float {
    if (!is_numeric($value) || ($number = (float) $value) < $minimum || $number > $maximum) lvmf_2026_json_error(400, "Invalid $name.");
    return $number;
}
function lvmf_2026_barycentric_height(array $vertices, float $x, float $y): ?float {
    // Fan triangulation honours all Appendix E BAA/BWSCA source vertices.
    if (count($vertices) < 3) return null;
    $first = $vertices[0];
    for ($i = 1; $i < count($vertices) - 1; $i++) {
        [$a, $b, $c] = [$first, $vertices[$i], $vertices[$i + 1]];
        $denominator = (($b['y'] - $c['y']) * ($a['x'] - $c['x'])) + (($c['x'] - $b['x']) * ($a['y'] - $c['y']));
        if (abs($denominator) < 0.0000001) continue;
        $u = ((($b['y'] - $c['y']) * ($x - $c['x'])) + (($c['x'] - $b['x']) * ($y - $c['y']))) / $denominator;
        $v = ((($c['y'] - $a['y']) * ($x - $c['x'])) + (($a['x'] - $c['x']) * ($y - $c['y']))) / $denominator;
        $w = 1 - $u - $v;
        if ($u >= -0.000001 && $v >= -0.000001 && $w >= -0.000001) return ($u * $a['z']) + ($v * $b['z']) + ($w * $c['z']);
    }
    return null;
}
