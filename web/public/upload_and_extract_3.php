<?php
// --- Config ---
$EXPECTED_API_KEY = 'bdb2b5a815c6db39cc8f175acfafbbad414c77024e2230f9511fd8dd55ab4d5a';
const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024;
const CONNECT_TIMEOUT    = 15;
const TRANSFER_TIMEOUT   = 300;
const MAX_REDIRECTS      = 5;
const EXTRACTOR_LOG      = '/var/log/pdf-extractor.log';
const JOB_PDF_DIR        = '/var/tmp/pdf-extract-jobs'; // <-- ensure writable by PHP

ini_set('log_errors', '1');
ini_set('display_errors', '0'); // don’t leak in prod

// --- DB bootstrap (Postgres) ---
function extractor_env(string $key, ?string $fallback = null): ?string
{
    $value = getenv($key);
    if ($value === false || $value === '') {
        return $fallback;
    }
    return $value;
}

function extractor_pg_pdo(): PDO
{
    static $pg = null;
    if ($pg instanceof PDO) {
        return $pg;
    }

    $legacyPgPhp = '/var/www/private/pg.php';
    $legacy = [];
    if (is_file($legacyPgPhp)) {
        require_once $legacyPgPhp;
        $legacy = [
            'host' => '127.0.0.1',
            'port' => isset($PG_PORT) ? $PG_PORT : null,
            'db' => isset($PG_DB) ? $PG_DB : null,
            'user' => isset($PG_USER) ? $PG_USER : null,
            'pass' => isset($PG_PASSWORD) ? $PG_PASSWORD : null,
            'sslmode' => 'disable',
        ];
    }

    $host = (string)($legacy['host'] ?? '127.0.0.1');
    $port = (string)($legacy['port'] ?? '5432');
    $db = (string)($legacy['db'] ?? 'docs_db');
    $user = (string)($legacy['user'] ?? 'webapp');
    $pass = (string)($legacy['pass'] ?? '');
    $sslmode = (string)($legacy['sslmode'] ?? 'disable');

    $dsn = sprintf('pgsql:host=%s;port=%s;dbname=%s;sslmode=%s', $host, $port, $db, $sslmode);
    $pg = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ]);
    return $pg;
}

try {
    $pg = extractor_pg_pdo();
} catch (Throwable $e) {
    die("Postgres connection failed: " . $e->getMessage());
}

// --- Fatal error safety net ---
register_shutdown_function(function () {
    $e = error_get_last();
    if ($e && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
        @file_put_contents(EXTRACTOR_LOG,
            sprintf("[%s] fatal %s\n", gmdate('c'), json_encode($e, JSON_UNESCAPED_SLASHES)),
            FILE_APPEND
        );
        if (!headers_sent()) {
            http_response_code(500);
            header('Content-Type: application/json; charset=utf-8');
        }
        echo json_encode(['success' => false, 'code' => 500, 'error' => 'Extractor fatal error. See server logs.']);
    }
});

// --- Headers ---
header('Content-Type: application/json; charset=utf-8');

// --- Logging ---
function log_ev(string $msg, array $ctx = []): void {
    $line = sprintf(
        "[%s] %s %s\n",
        gmdate('c'),
        $msg,
        $ctx ? json_encode($ctx, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES) : ''
    );
    @file_put_contents(EXTRACTOR_LOG, $line, FILE_APPEND);
    @openlog('pdf-extractor', LOG_ODELAY, LOG_USER);
    @syslog(LOG_INFO, $msg . ($ctx ? ' ' . json_encode($ctx) : ''));
    @closelog();
}

function respond(int $httpCode, array $payload): void {
    log_ev('respond', ['http' => $httpCode, 'ok' => ($httpCode>=200 && $httpCode<300)]);
    http_response_code($httpCode);
    if (!isset($payload['success'])) {
        $payload = ['success' => ($httpCode >= 200 && $httpCode < 300)] + $payload;
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE);
    exit;
}

// --- Helpers -------------------------------------------------------------

function uploadErrorMessage(int $err): string {
    return match ($err) {
        UPLOAD_ERR_INI_SIZE      => 'File exceeds upload_max_filesize.',
        UPLOAD_ERR_FORM_SIZE     => 'File exceeds MAX_FILE_SIZE form limit.',
        UPLOAD_ERR_PARTIAL       => 'File was only partially uploaded.',
        UPLOAD_ERR_NO_FILE       => 'No file was uploaded.',
        UPLOAD_ERR_NO_TMP_DIR    => 'Missing a temporary folder on server.',
        UPLOAD_ERR_CANT_WRITE    => 'Failed to write file to disk.',
        UPLOAD_ERR_EXTENSION     => 'A PHP extension stopped the file upload.',
        default                  => 'Unknown upload error.'
    };
}

function has_cmd(string $cmd): bool {
    $out = []; $ret = 0;
    @exec('command -v '.escapeshellarg($cmd).' 2>/dev/null', $out, $ret);
    return $ret === 0 && !empty($out);
}

function pdf_page_count(string $pdfPath): ?int {
    $cmd = 'pdfinfo '.escapeshellarg($pdfPath).' 2>/dev/null';
    $out = []; $ret = 0;
    exec($cmd, $out, $ret);
    if ($ret !== 0) return null;
    foreach ($out as $line) {
        if (stripos($line, 'Pages:') === 0) return (int)trim(substr($line, 6));
    }
    return null;
}

function page_has_text(string $pdfPath, int $page): bool {
    $cmd = 'pdftotext -layout -f '.(int)$page.' -l '.(int)$page.' '.escapeshellarg($pdfPath).' - 2>/dev/null';
    $out = []; $ret = 0;
    exec($cmd, $out, $ret);
    $txt = trim(implode("\n", $out));
    return ($ret === 0) && ($txt !== '');
}

/** SSRF guard: allow only http/https; ban loopback/private IPs */
function is_private_ip(string $ip): bool {
    if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
        $long = ip2long($ip);
        $ranges = [
            ['10.0.0.0',   '10.255.255.255'],
            ['172.16.0.0', '172.31.255.255'],
            ['192.168.0.0','192.168.255.255'],
            ['127.0.0.0',  '127.255.255.255'],
        ];
        foreach ($ranges as [$start, $end]) {
            if ($long >= ip2long($start) && $long <= ip2long($end)) return true;
        }
        return false;
    }
    if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
        if ($ip === '::1') return true;
        $l = strtolower($ip);
        if (str_starts_with($l, 'fc') || str_starts_with($l, 'fd')) return true;
        if (str_starts_with($l, 'fe80')) return true;
    }
    return false;
}

/** Download a remote PDF to a temp file with size/timeout/redirect limits */
function download_pdf_from_url(string $url, string $dest, int $maxBytes = MAX_DOWNLOAD_BYTES): array {
    $parts = parse_url($url);
    if (!$parts || !in_array(strtolower($parts['scheme'] ?? ''), ['http','https'], true)) {
        return ['ok' => false, 'error' => 'Only http/https URLs are allowed'];
    }

    $host = $parts['host'] ?? '';
    $ip   = @gethostbyname($host);
    if ($ip && filter_var($ip, FILTER_VALIDATE_IP) && is_private_ip($ip)) {
        return ['ok' => false, 'error' => 'Refusing to fetch private/loopback host'];
    }

    $fh = @fopen($dest, 'wb');
    if (!$fh) return ['ok' => false, 'error' => 'Cannot open temp file for writing'];

    $downloaded = 0; $httpCode = null; $finalMime = null;
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => MAX_REDIRECTS,
        CURLOPT_CONNECTTIMEOUT => CONNECT_TIMEOUT,
        CURLOPT_TIMEOUT        => TRANSFER_TIMEOUT,
        CURLOPT_FILE           => $fh,
        CURLOPT_NOPROGRESS     => false,
        CURLOPT_USERAGENT      => 'pdf-extractor/1.0',
        CURLOPT_HEADERFUNCTION => function($ch, $header) use (&$finalMime) {
            $len = strlen($header);
            if (stripos($header, 'Content-Type:') === 0) {
                $finalMime = trim(substr($header, 13));
            }
            return $len;
        },
        CURLOPT_PROGRESSFUNCTION => function($resource, $dl_total, $dl_now) use (&$downloaded, $maxBytes) {
            $downloaded = (int)$dl_now;
            return ($dl_now > $maxBytes) ? 1 : 0;
        },
        CURLOPT_HTTPHEADER     => ['Expect:'],
    ]);
    $ok = curl_exec($ch);
    $curlErr = curl_error($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    fflush($fh); fclose($fh);

    if (!$ok) {
        @unlink($dest);
        return ['ok' => false, 'error' => "cURL error: $curlErr", 'http' => $httpCode];
    }
    if ($httpCode < 200 || $httpCode >= 300) {
        @unlink($dest);
        return ['ok' => false, 'error' => "HTTP $httpCode while fetching URL", 'http' => $httpCode];
    }

    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mime  = $finfo ? finfo_file($finfo, $dest) : null;
    if ($finfo) finfo_close($finfo);

    $isPdfSig = false;
    $h = @fopen($dest, 'rb');
    if ($h) {
        $sig = fread($h, 5);
        fclose($h);
        $isPdfSig = ($sig === "%PDF-");
    }

    if (!$isPdfSig || ($mime && $mime !== 'application/pdf' && $mime !== 'application/octet-stream')) {
        @unlink($dest);
        return ['ok' => false, 'error' => 'Downloaded file is not a valid PDF'];
    }

    return ['ok' => true, 'mime' => 'application/pdf'];
}

// --- NEW: Job helpers (DB) -----------------------------------------------

/** Enqueue OCR job: move PDF to stable path, insert into extract_jobs, return job_id */
function enqueue_extract_job(PDO $pg, string $pdfPath, array $source, ?string $sha256, ?int $bytes): int {
    if (!is_dir(JOB_PDF_DIR)) {
        @mkdir(JOB_PDF_DIR, 0770, true);
    }
    $stablePath = rtrim(JOB_PDF_DIR, '/') . '/' . uniqid('job_', true) . '.pdf';
    if (!@rename($pdfPath, $stablePath)) {
        log_ev('enqueue_move_failed', ['from' => $pdfPath, 'to' => $stablePath]);
        respond(500, ['error' => 'Failed to move PDF into job directory.', 'code' => 500]);
    }

    $stmt = $pg->prepare("
        INSERT INTO extract_jobs (status, progress, source_mode, source_info, pdf_sha256, pdf_bytes, pdf_path)
        VALUES ('queued', 0, :mode, CAST(:info AS jsonb), :sha, :bytes, :path)
        RETURNING id
    ");
    $stmt->execute([
        ':mode'  => $source['mode'] ?? null,
        ':info'  => json_encode($source, JSON_UNESCAPED_UNICODE),
        ':sha'   => $sha256,
        ':bytes' => $bytes,
        ':path'  => $stablePath,
    ]);
    $jobId = (int)$stmt->fetchColumn();
    log_ev('enqueue_job_ok', ['job_id' => $jobId, 'path' => $stablePath]);
    return $jobId;
}

function fetch_job(PDO $pg, int $jobId): ?array {
    $stmt = $pg->prepare("SELECT * FROM extract_jobs WHERE id = :id");
    $stmt->execute([':id' => $jobId]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

function handle_status_request(PDO $pg, int $jobId): void {
    $job = fetch_job($pg, $jobId);
    if (!$job) {
        respond(404, ['error' => 'Job not found', 'code' => 404]);
    }

    $status = $job['status'];
    $progress = (int)($job['progress'] ?? 0);

    if ($status === 'queued' || $status === 'running') {
        respond(200, [
            'status'     => $status,
            'progress'   => $progress,
            'text_ready' => false,
            'ocr_used'   => (bool)$job['ocr_used'],
            'ocr_pages'  => $job['ocr_pages'] ? json_decode($job['ocr_pages'], true) : [],
        ]);
    } elseif ($status === 'completed') {
        respond(200, [
            'status'     => 'completed',
            'progress'   => 100,
            'text_ready' => true,
            'text'       => $job['text_result'],
            'ocr_used'   => (bool)$job['ocr_used'],
            'ocr_pages'  => $job['ocr_pages'] ? json_decode($job['ocr_pages'], true) : [],
            'sha256'     => $job['pdf_sha256'],
            'bytes'      => $job['pdf_bytes'],
            'source'     => $job['source_info'] ? json_decode($job['source_info'], true) : null,
        ]);
    } else { // failed or unknown
        respond(500, [
            'status'   => $status,
            'progress' => $progress,
            'error'    => $job['error_message'] ?? 'Job failed',
        ]);
    }
}

// --- Auth (accept api_key via GET or POST) -------------------------------
$apiKey = $_POST['api_key'] ?? $_GET['api_key'] ?? '';
if ($apiKey !== $EXPECTED_API_KEY) {
    log_ev('auth_fail', ['remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null]);
    respond(403, ['error' => 'Access denied.', 'code' => 403]);
}

// ensure $pg is usable
if (!isset($pg) || !$pg instanceof PDO) {
    log_ev('no_pg_connection');
    respond(500, ['error' => 'Database connection not available for jobs.', 'code' => 500]);
}

// --- Status polling mode (GET ?job_id=123) -------------------------------
if ($_SERVER['REQUEST_METHOD'] === 'GET' && isset($_GET['job_id'])) {
    $jobId = (int)$_GET['job_id'];
    log_ev('status_request', ['job_id' => $jobId, 'remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null]);
    handle_status_request($pg, $jobId);
}

// --- Source selection (normal extract mode) -------------------------------
$hasUpload = isset($_FILES['pdf']);
$pdfUrl    = trim((string)($_POST['pdf_url'] ?? ''));
log_ev('request_begin', [
    'mode' => $hasUpload ? 'upload' : ($pdfUrl ? 'url' : 'none'),
    'remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null,
    'content_length' => $_SERVER['CONTENT_LENGTH'] ?? null,
    'user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? null
]);

$ts0 = microtime(true);
if (!$hasUpload && $pdfUrl === '') {
    respond(400, ['error' => 'Provide either a PDF upload (pdf) or pdf_url.', 'code' => 400]);
}

// --- Prepare temp paths ---
$tempDir  = sys_get_temp_dir();
$baseName = uniqid('upload_', true) . '.pdf';
$pdfPath  = $tempDir . '/' . $baseName;
$textPath = $pdfPath . '.txt';

$cleanup = function() use ($pdfPath, $textPath) {
    if (is_file($pdfPath)) @unlink($pdfPath);
    if (is_file($textPath)) @unlink($textPath);
};

$source    = null;
$mimeType  = null;

// --- Acquire PDF ---
if ($hasUpload) {
    if (!empty($_FILES['pdf']['error'])) {
        log_ev('upload_err', ['code' => (int)$_FILES['pdf']['error']]);
        respond(400, ['error' => uploadErrorMessage((int)$_FILES['pdf']['error']), 'code' => 400]);
    }
    $uploadedFile = $_FILES['pdf']['tmp_name'];
    $originalName = basename($_FILES['pdf']['name'] ?? 'document.pdf');

    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mimeType = $finfo ? finfo_file($finfo, $uploadedFile) : null;
    if ($finfo) finfo_close($finfo);

    if ($mimeType !== 'application/pdf') {
        log_ev('upload_bad_mime', ['mime' => $mimeType]);
        respond(400, ['error' => 'Not a valid PDF file.', 'code' => 400]);
    }
    if (!move_uploaded_file($uploadedFile, $pdfPath)) {
        log_ev('move_uploaded_failed');
        $cleanup(); respond(500, ['error' => 'Failed to move uploaded file.', 'code' => 500]);
    }
    $source = ['mode' => 'upload', 'filename' => $originalName, 'size' => @filesize($pdfPath)];
    log_ev('upload_ok', $source);
} else {
    $dlStart = microtime(true);
    $dl = download_pdf_from_url($pdfUrl, $pdfPath, MAX_DOWNLOAD_BYTES);
    $dlTime = round(microtime(true) - $dlStart, 3);
    if (!($dl['ok'] ?? false)) {
        log_ev('url_fetch_fail', ['url' => $pdfUrl, 'error' => $dl['error'] ?? 'unknown', 'http' => $dl['http'] ?? null, 'dur_s' => $dlTime]);
        $cleanup();
        respond(400, ['error' => 'Failed to fetch pdf_url: ' . ($dl['error'] ?? 'unknown'), 'code' => (int)($dl['http'] ?? 400)]);
    }
    $mimeType = 'application/pdf';
    $source   = ['mode' => 'url', 'url' => $pdfUrl, 'size' => @filesize($pdfPath), 'dur_s' => $dlTime];
    log_ev('url_fetch_ok', $source);
}

$sha256 = @hash_file('sha256', $pdfPath) ?: null;
$bytes  = @filesize($pdfPath) ?: null;

// --- First pass pdftotext (cheap-ish) ------------------------------------
$convStart = microtime(true);
$tmpTextPath = $pdfPath . '.tmp.txt';
$cmd = "pdftotext -layout " . escapeshellarg($pdfPath) . " " . escapeshellarg($tmpTextPath) . " 2>&1";
$output = []; $returnVar = 0;
exec($cmd, $output, $returnVar);
$initialText = (is_file($tmpTextPath) ? (string)file_get_contents($tmpTextPath) : '');
log_ev('pdftotext_initial_done', ['ret' => $returnVar, 'bytes' => strlen($initialText)]);

// --- Decide if OCR is needed (for async vs sync) -------------------------
$needOcr = false;
$ocrPagesProbe = [];

if (!has_cmd('ocrmypdf')) {
    log_ev('ocr_unavailable_for_http');
} else {
    if (has_cmd('pdfinfo') && has_cmd('pdftotext')) {
        $pages = pdf_page_count($pdfPath);
        if ($pages && $pages > 0) {
            // You can cap to first N pages if you want to cheapen it
            $maxProbe = min($pages, 10);
            for ($p = 1; $p <= $maxProbe; $p++) {
                if (!page_has_text($pdfPath, $p)) {
                    $ocrPagesProbe[] = $p;
                }
            }
            $needOcr = count($ocrPagesProbe) > 0 || trim($initialText) === '';
            log_ev('probe_pages_http', ['pages' => $pages, 'probed' => $maxProbe, 'empty_pages' => $ocrPagesProbe, 'heuristic_needOcr' => $needOcr]);
        } else {
            $needOcr = (trim($initialText) === '');
            log_ev('probe_unknown_pages_http', ['needOcrHeuristic' => $needOcr]);
        }
    } else {
        $needOcr = (trim($initialText) === '');
        log_ev('probe_tools_missing_http', ['needOcrHeuristic' => $needOcr]);
    }
}

// If OCR needed → enqueue job and return 202 with job_id; do NOT run OCR here
if ($needOcr) {
    if (!is_file($pdfPath)) {
        $cleanup();
        respond(500, ['error' => 'PDF file missing before enqueue.', 'code' => 500]);
    }

    // We don't need the tmp text file for the worker; delete it
    if (is_file($tmpTextPath)) {
        @unlink($tmpTextPath);
    }

    $jobId = enqueue_extract_job($pg, $pdfPath, $source ?? [], $sha256, $bytes);

    log_ev('request_enqueued', [
        'job_id' => $jobId,
        'dur_s'  => round(microtime(true) - $ts0, 3),
        'source_mode' => $source['mode'] ?? null
    ]);

    respond(202, [
        'status'  => 'queued',
        'job_id'  => $jobId,
        'ocr'     => true,
        'message' => 'OCR required; job queued. Poll this endpoint with ?job_id=JOB_ID.',
    ]);
}

// --- No OCR needed: finish synchronously as before -----------------------
if ($returnVar !== 0 || !is_file($tmpTextPath)) {
    $cleanup();
    log_ev('fatal_pdftotext_failed_sync', ['ret' => $returnVar, 'out' => implode("\n", $output)]);
    respond(500, ['error' => 'PDF conversion failed.', 'code' => 500]);
}

// move tmp text into final path and return
rename($tmpTextPath, $textPath);
$text = (string)file_get_contents($textPath);

log_ev('request_end_sync', [
    'dur_s'      => round(microtime(true) - $ts0, 3),
    'bytes_text' => strlen($text),
    'ocr_used'   => false,
    'source_mode'=> $source['mode'] ?? null
]);

// cleanup
@unlink($textPath);
$cleanup();

respond(200, [
    'text'      => $text,
    'mime'      => $mimeType ?? 'application/pdf',
    'source'    => $source ?? null,
    'ocr_used'  => false,
    'ocr_pages' => [],
    'sha256'    => $sha256,
    'bytes'     => $bytes,
]);
