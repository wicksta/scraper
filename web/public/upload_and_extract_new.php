<?php
// --- Config ---
$EXPECTED_API_KEY = 'bdb2b5a815c6db39cc8f175acfafbbad414c77024e2230f9511fd8dd55ab4d5a';
const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024;
const CONNECT_TIMEOUT    = 15;
const TRANSFER_TIMEOUT   = 300;
const MAX_REDIRECTS      = 5;
const EXTRACTOR_LOG      = '/var/log/pdf-extractor.log';

ini_set('log_errors', '1');
ini_set('display_errors', '0'); // don’t leak in prod

register_shutdown_function(function () {
    $e = error_get_last();
    if ($e && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
        // best-effort logging
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
// header('Access-Control-Allow-Origin: *'); // enable if needed

// --- Logging ---
function log_ev(string $msg, array $ctx = []): void {
    $line = sprintf(
        "[%s] %s %s\n",
        gmdate('c'),
        $msg,
        $ctx ? json_encode($ctx, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES) : ''
    );
    // File log
    @file_put_contents(EXTRACTOR_LOG, $line, FILE_APPEND);
    // Syslog (best-effort)
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

function normalize_text(string $text): string {
    $text = str_replace(["\r\n", "\r"], "\n", $text);
    $text = preg_replace("/\n{3,}/", "\n\n", $text) ?? $text;
    return trim($text);
}

function detect_doc_kind(string $path, ?string $mime, string $originalName = ''): ?string {
    $ext = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
    if ($ext === 'pdf') return 'pdf';
    if ($ext === 'docx') return 'docx';
    if ($ext === 'doc') return 'doc';

    $m = strtolower((string)$mime);
    if ($m === 'application/pdf') return 'pdf';
    if ($m === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document') return 'docx';
    if ($m === 'application/msword' || $m === 'application/vnd.ms-word') return 'doc';

    $fh = @fopen($path, 'rb');
    if (!$fh) return null;
    $sig = (string)fread($fh, 8);
    fclose($fh);

    if (strncmp($sig, '%PDF-', 5) === 0) return 'pdf';
    if (strncmp($sig, "PK\x03\x04", 4) === 0 && class_exists('ZipArchive')) {
        $zip = new ZipArchive();
        if ($zip->open($path) === true) {
            $isDocx = ($zip->locateName('word/document.xml') !== false);
            $zip->close();
            if ($isDocx) return 'docx';
        }
    }
    if ($sig === "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1") return 'doc';
    return null;
}

function extract_docx_text_from_zip(string $path): array {
    if (!class_exists('ZipArchive')) {
        return ['success' => false, 'error' => 'ZipArchive is unavailable for DOCX extraction.'];
    }
    $zip = new ZipArchive();
    if ($zip->open($path) !== true) {
        return ['success' => false, 'error' => 'Unable to open DOCX file.'];
    }

    $parts = ['word/document.xml'];
    for ($i = 1; $i <= 10; $i++) {
        $parts[] = "word/header{$i}.xml";
        $parts[] = "word/footer{$i}.xml";
    }

    $combined = '';
    foreach ($parts as $part) {
        $xml = $zip->getFromName($part);
        if (!is_string($xml) || $xml === '') continue;
        $xml = preg_replace('/<w:p\b[^>]*>/', "\n", $xml) ?? $xml;
        $xml = preg_replace('/<w:tab\b[^>]*\/>/', "\t", $xml) ?? $xml;
        $xml = preg_replace('/<w:br\b[^>]*\/>/', "\n", $xml) ?? $xml;
        $xml = preg_replace('/<w:cr\b[^>]*\/>/', "\n", $xml) ?? $xml;
        $txt = strip_tags($xml);
        if ($txt !== '') {
            $combined .= "\n" . html_entity_decode($txt, ENT_QUOTES | ENT_XML1, 'UTF-8');
        }
    }
    $zip->close();

    $combined = normalize_text($combined);
    if ($combined === '') {
        return ['success' => false, 'error' => 'DOCX extracted no readable text.'];
    }
    return ['success' => true, 'text' => $combined];
}

function extract_doc_text_via_cmd(string $path): array {
    if (has_cmd('antiword')) {
        $cmd = 'antiword ' . escapeshellarg($path) . ' 2>/dev/null';
        $out = []; $ret = 0;
        exec($cmd, $out, $ret);
        $txt = normalize_text(implode("\n", $out));
        if ($ret === 0 && $txt !== '') return ['success' => true, 'text' => $txt];
    }
    if (has_cmd('catdoc')) {
        $cmd = 'catdoc ' . escapeshellarg($path) . ' 2>/dev/null';
        $out = []; $ret = 0;
        exec($cmd, $out, $ret);
        $txt = normalize_text(implode("\n", $out));
        if ($ret === 0 && $txt !== '') return ['success' => true, 'text' => $txt];
    }
    return ['success' => false, 'error' => 'No DOC extractor available (antiword/catdoc).'];
}

/** Very small SSRF guard: allow only http/https; ban loopback/private IPs */
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
        // Treat ::1 and fc00::/7 as private/link-local
        if ($ip === '::1') return true;
        if (str_starts_with(strtolower($ip), 'fc') || str_starts_with(strtolower($ip), 'fd')) return true;
        if (str_starts_with(strtolower($ip), 'fe80')) return true;
    }
    return false;
}

/** Download a remote PDF/DOCX/DOC to a temp file with size/timeout/redirect limits */
function download_pdf_from_url(string $url, string $dest, int $maxBytes = MAX_DOWNLOAD_BYTES): array {
    $parts = parse_url($url);
    if (!$parts || !in_array(strtolower($parts['scheme'] ?? ''), ['http','https'], true)) {
        return ['ok' => false, 'error' => 'Only http/https URLs are allowed'];
    }

    // Basic SSRF guard: resolve and reject private/loopback addresses
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
            return ($dl_now > $maxBytes) ? 1 : 0; // abort if exceeding
        },
        CURLOPT_HTTPHEADER     => ['Expect:'], // disable 100-continue
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

    // Validate that we actually got a supported document (best-effort)
    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mime  = $finfo ? finfo_file($finfo, $dest) : null;
    if ($finfo) finfo_close($finfo);

    $kind = detect_doc_kind($dest, $mime, basename(parse_url($url, PHP_URL_PATH) ?: ''));
    if (!in_array($kind, ['pdf', 'docx', 'doc'], true)) {
        @unlink($dest);
        return ['ok' => false, 'error' => 'Downloaded file is not a valid PDF/DOCX/DOC'];
    }

    $resolvedMime = match ($kind) {
        'docx' => 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc'  => 'application/msword',
        default => 'application/pdf',
    };
    return ['ok' => true, 'mime' => $resolvedMime, 'kind' => $kind];
}

// --- Auth ---
$apiKey = $_POST['api_key'] ?? '';
if ($apiKey !== $EXPECTED_API_KEY) {
    log_ev('auth_fail', ['remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null]);
    respond(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
}

// --- Source selection ---
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
    respond(400, ['success' => false, 'error' => 'Provide either a PDF upload (pdf) or pdf_url.', 'code' => 400]);
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
$docKind   = null;
$ocrUsed   = false;
$ocrPages  = [];
$ocrPdf    = $pdfPath . '.ocr.pdf';
$textTmp   = $pdfPath . '.tmp.txt';
$textFinal = $textPath;

// --- Acquire document ---
if ($hasUpload) {
    if (!empty($_FILES['pdf']['error'])) {
        log_ev('upload_err', ['code' => (int)$_FILES['pdf']['error']]);
        respond(400, ['success' => false, 'error' => uploadErrorMessage((int)$_FILES['pdf']['error']), 'code' => 400]);
    }
    $uploadedFile = $_FILES['pdf']['tmp_name'];
    $originalName = basename($_FILES['pdf']['name'] ?? 'document.pdf');

    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mimeType = $finfo ? finfo_file($finfo, $uploadedFile) : null;
    if ($finfo) finfo_close($finfo);

    $docKind = detect_doc_kind($uploadedFile, $mimeType, $originalName);
    if (!in_array($docKind, ['pdf', 'docx', 'doc'], true)) {
        log_ev('upload_bad_type', ['mime' => $mimeType, 'name' => $originalName]);
        respond(400, ['success' => false, 'error' => 'Not a valid PDF/DOCX/DOC file.', 'code' => 400]);
    }
    if (!move_uploaded_file($uploadedFile, $pdfPath)) {
        log_ev('move_uploaded_failed');
        $cleanup(); respond(500, ['success' => false, 'error' => 'Failed to move uploaded file.', 'code' => 500]);
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
        respond(400, ['success' => false, 'error' => 'Failed to fetch pdf_url: ' . ($dl['error'] ?? 'unknown'), 'code' => (int)($dl['http'] ?? 400)]);
    }
    $mimeType = $dl['mime'] ?? 'application/pdf';
    $docKind  = $dl['kind'] ?? null;
    $source   = ['mode' => 'url', 'url' => $pdfUrl, 'size' => @filesize($pdfPath), 'dur_s' => $dlTime];
    log_ev('url_fetch_ok', $source);
}

$docKind = $docKind ?: detect_doc_kind($pdfPath, $mimeType, (string)($source['filename'] ?? ''));
if (!in_array($docKind, ['pdf', 'docx', 'doc'], true)) {
    $cleanup();
    respond(400, ['success' => false, 'error' => 'Could not determine document type.', 'code' => 400]);
}

$sha256 = @hash_file('sha256', $pdfPath) ?: null;
$bytes  = @filesize($pdfPath) ?: null;

if ($docKind === 'pdf') {
    // --- pdftotext ---
    $cmd = "pdftotext -layout " . escapeshellarg($pdfPath) . " " . escapeshellarg($pdfPath . '.tmp.txt') . " 2>&1";
    $output = []; $returnVar = 0;
    exec($cmd, $output, $returnVar);
    $initialText = (is_file($pdfPath . '.tmp.txt') ? (string)file_get_contents($pdfPath . '.tmp.txt') : '');
    log_ev('pdftotext_done', ['ret' => $returnVar, 'bytes' => strlen($initialText)]);

    // --- OCR decision ---
    $ocrUsed = false; $ocrPages = [];
    $needOcr = false;
    if (!has_cmd('ocrmypdf')) {
        log_ev('ocr_unavailable');
    } else {
        if (has_cmd('pdfinfo') && has_cmd('pdftotext')) {
            $pages = pdf_page_count($pdfPath);
            $probeStart = microtime(true);
            if ($pages && $pages > 0) {
                for ($p = 1; $p <= $pages; $p++) {
                    if (!page_has_text($pdfPath, $p)) $ocrPages[] = $p;
                }
                $needOcr = count($ocrPages) > 0;
                log_ev('probe_pages', ['pages' => $pages, 'ocr_pages' => $ocrPages, 'dur_s' => round(microtime(true)-$probeStart,3)]);
            } else {
                $needOcr = (trim($initialText) === '');
                log_ev('probe_unknown_pages', ['needOcrHeuristic' => $needOcr]);
            }
        } else {
            $needOcr = (trim($initialText) === '');
            log_ev('probe_tools_missing', ['needOcrHeuristic' => $needOcr]);
        }
    }

    // --- OCR + re-extract or finalize ---
    $textFinal = $textPath;
    if ($needOcr) {
        $ocrStart = microtime(true);
        $ocrPdf = $pdfPath . '.ocr.pdf';
        $ocrCmd = 'ocrmypdf -l eng --skip-text --optimize 0 ' . escapeshellarg($pdfPath) . ' ' . escapeshellarg($ocrPdf) . ' 2>&1';
        $ocrOut = []; $ocrRet = 0;
        exec($ocrCmd, $ocrOut, $ocrRet);
        log_ev('ocr_done', ['ret' => $ocrRet, 'dur_s' => round(microtime(true)-$ocrStart,3)]);

        if ($ocrRet === 0 && is_file($ocrPdf)) {
            $ocrUsed = true;
            $reCmd = "pdftotext -layout " . escapeshellarg($ocrPdf) . " " . escapeshellarg($textFinal) . " 2>&1";
            $reOut = []; $reRet = 0;
            exec($reCmd, $reOut, $reRet);
            log_ev('pdftotext_after_ocr', ['ret' => $reRet]);
            if ($reRet !== 0 || !is_file($textFinal)) {
                if ($initialText !== '') file_put_contents($textFinal, $initialText);
            }
        } else {
            if ($initialText !== '') {
                file_put_contents($textFinal, $initialText);
            } else {
                @unlink($pdfPath . '.tmp.txt'); @unlink($ocrPdf);
                $cleanup();
                log_ev('fatal_ocr_failed', ['ocr_out' => implode("\n", $ocrOut)]);
                respond(500, ['success' => false, 'error' => 'PDF conversion failed (OCR error).', 'code' => 500]);
            }
        }
    } else {
        if ($returnVar !== 0 || !is_file($pdfPath . '.tmp.txt')) {
            $cleanup();
            log_ev('fatal_pdftotext_failed', ['ret' => $returnVar, 'out' => implode("\n", $output)]);
            respond(500, ['success' => false, 'error' => 'PDF conversion failed.', 'code' => 500]);
        }
        rename($pdfPath . '.tmp.txt', $textFinal);
    }
    $text = (string)file_get_contents($textFinal);
} elseif ($docKind === 'docx') {
    $res = extract_docx_text_from_zip($pdfPath);
    if (empty($res['success'])) {
        $cleanup();
        log_ev('fatal_docx_extract_failed', ['error' => $res['error'] ?? 'unknown']);
        respond(500, ['success' => false, 'error' => (string)($res['error'] ?? 'DOCX conversion failed.'), 'code' => 500]);
    }
    $text = (string)($res['text'] ?? '');
    $ocrUsed = false;
    $ocrPages = [];
} else { // doc
    $res = extract_doc_text_via_cmd($pdfPath);
    if (empty($res['success'])) {
        $cleanup();
        log_ev('fatal_doc_extract_failed', ['error' => $res['error'] ?? 'unknown']);
        respond(500, ['success' => false, 'error' => (string)($res['error'] ?? 'DOC conversion failed.'), 'code' => 500]);
    }
    $text = (string)($res['text'] ?? '');
    $ocrUsed = false;
    $ocrPages = [];
}

if (trim($text) === '') {
    $cleanup();
    respond(500, ['success' => false, 'error' => 'Extraction produced empty text.', 'code' => 500]);
}
log_ev('request_end', [
    'dur_s' => round(microtime(true) - $ts0, 3),
    'bytes_text' => strlen($text),
    'ocr_used' => $ocrUsed,
    'source_mode' => $source['mode'] ?? null,
    'doc_kind' => $docKind
]);

// cleanup temp files (keep your cleanup())
@unlink($pdfPath . '.tmp.txt');
@unlink($pdfPath . '.ocr.pdf');
$cleanup();

respond(200, [
    'success'   => true,
    'text'      => $text,
    'mime'      => $mimeType ?? 'application/pdf',
    'source'    => $source ?? null,
    'ocr_used'  => $ocrUsed,
    'ocr_pages' => $ocrPages,
    'sha256'    => $sha256,
    'bytes'     => $bytes,
]);
