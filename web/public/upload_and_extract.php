<?php
// --- Config ---
$EXPECTED_API_KEY = 'bdb2b5a815c6db39cc8f175acfafbbad414c77024e2230f9511fd8dd55ab4d5a';

// --- Headers ---
header('Content-Type: application/json; charset=utf-8');
// Optional CORS (enable if you call from browsers on other origins)
// header('Access-Control-Allow-Origin: *');

// --- Helpers ---
function respond(int $httpCode, array $payload): void {
    http_response_code($httpCode);
    // Ensure 'success' is present
    if (!isset($payload['success'])) {
        $payload = array_merge(['success' => ($httpCode >= 200 && $httpCode < 300)], $payload);
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE);
    exit;
}

function uploadErrorMessage(int $err): string {
    // PHP file upload error codes
    return match ($err) {
        UPLOAD_ERR_INI_SIZE => 'File exceeds upload_max_filesize.',
        UPLOAD_ERR_FORM_SIZE => 'File exceeds MAX_FILE_SIZE form limit.',
        UPLOAD_ERR_PARTIAL => 'File was only partially uploaded.',
        UPLOAD_ERR_NO_FILE => 'No file was uploaded.',
        UPLOAD_ERR_NO_TMP_DIR => 'Missing a temporary folder on server.',
        UPLOAD_ERR_CANT_WRITE => 'Failed to write file to disk.',
        UPLOAD_ERR_EXTENSION => 'A PHP extension stopped the file upload.',
        default => 'Unknown upload error.'
    };
}

// --- Auth ---
$apiKey = $_POST['api_key'] ?? '';
if ($apiKey !== $EXPECTED_API_KEY) {
    respond(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
}

// --- Validate file field ---
if (!isset($_FILES['pdf'])) {
    respond(400, ['success' => false, 'error' => 'No PDF uploaded.', 'code' => 400]);
}

if (!empty($_FILES['pdf']['error'])) {
    respond(400, ['success' => false, 'error' => uploadErrorMessage((int)$_FILES['pdf']['error']), 'code' => 400]);
}

$uploadedFile = $_FILES['pdf']['tmp_name'];
$originalName = basename($_FILES['pdf']['name'] ?? 'document.pdf');

// --- MIME check ---
$finfo = finfo_open(FILEINFO_MIME_TYPE);
if (!$finfo) {
    respond(500, ['success' => false, 'error' => 'Server finfo unavailable.', 'code' => 500]);
}
$mimeType = finfo_file($finfo, $uploadedFile);
finfo_close($finfo);

if (!$mimeType) {
    respond(400, ['success' => false, 'error' => 'Unable to determine MIME type.', 'code' => 400]);
}
if ($mimeType !== 'application/pdf') {
    respond(400, ['success' => false, 'error' => 'Not a valid PDF file.', 'code' => 400]);
}

// --- Prepare temp paths ---
$tempDir  = sys_get_temp_dir();
$baseName = uniqid('upload_', true) . '.pdf';
$pdfPath  = $tempDir . '/' . $baseName;
$textPath = $pdfPath . '.txt';

// Ensure cleanup on any exit
$cleanup = function() use ($pdfPath, $textPath) {
    if (is_file($pdfPath)) @unlink($pdfPath);
    if (is_file($textPath)) @unlink($textPath);
};

// --- Move upload ---
if (!move_uploaded_file($uploadedFile, $pdfPath)) {
    $cleanup();
    respond(500, ['success' => false, 'error' => 'Failed to move uploaded file.', 'code' => 500]);
}

// --- Convert to text ---
$cmd = "pdftotext -layout " . escapeshellarg($pdfPath) . " " . escapeshellarg($textPath) . " 2>&1";
$output = [];
$returnVar = 0;
exec($cmd, $output, $returnVar);

// --- Check result ---
if ($returnVar !== 0 || !is_file($textPath)) {
    $cleanup();
    respond(500, [
        'success' => false,
        'error' => 'PDF conversion failed.',
        'code' => 500,
        // You can comment out 'details' in production if you don’t want to leak internals
        'details' => [
            'return' => $returnVar,
            'output' => implode("\n", $output)
        ]
    ]);
}

// --- Read and return text ---
$text = file_get_contents($textPath);
$cleanup();

respond(200, [
    'success' => true,
    'text'    => $text,
    'filename'=> $originalName,
    'mime'    => $mimeType
]);




/*

<?php
// --- Config ---
$EXPECTED_API_KEY = 'bdb2b5a815c6db39cc8f175acfafbbad414c77024e2230f9511fd8dd55ab4d5a';

const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024; // 200 MB cap for pdf_url
const CONNECT_TIMEOUT    = 15;   // seconds
const TRANSFER_TIMEOUT   = 300;  // seconds
const MAX_REDIRECTS      = 5;

// --- Headers ---
header('Content-Type: application/json; charset=utf-8');
// header('Access-Control-Allow-Origin: *'); // enable if needed

// --- Helpers ---
function respond(int $httpCode, array $payload): void {
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

/** Download a remote PDF to a temp file with size/timeout/redirect limits */
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

    // Validate that we actually got a PDF (best-effort)
    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mime  = $finfo ? finfo_file($finfo, $dest) : null;
    if ($finfo) finfo_close($finfo);

    // Accept application/pdf or octet-stream if the file *is* a PDF by signature
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

// --- Auth ---
$apiKey = $_POST['api_key'] ?? '';
if ($apiKey !== $EXPECTED_API_KEY) {
    respond(403, ['success' => false, 'error' => 'Access denied.', 'code' => 403]);
}

// --- Source selection: upload or url ---
$hasUpload = isset($_FILES['pdf']);
$pdfUrl    = trim((string)($_POST['pdf_url'] ?? ''));

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
$ocrUsed   = false;
$ocrPages  = [];
$ocrPdf    = $pdfPath . '.ocr.pdf';
$textTmp   = $pdfPath . '.tmp.txt';
$textFinal = $textPath;

// --- Acquire PDF into $pdfPath ---
if ($hasUpload) {
    if (!empty($_FILES['pdf']['error'])) {
        respond(400, ['success' => false, 'error' => uploadErrorMessage((int)$_FILES['pdf']['error']), 'code' => 400]);
    }
    $uploadedFile = $_FILES['pdf']['tmp_name'];
    $originalName = basename($_FILES['pdf']['name'] ?? 'document.pdf');

    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    if (!$finfo) { respond(500, ['success' => false, 'error' => 'Server finfo unavailable.', 'code' => 500]); }
    $mimeType = finfo_file($finfo, $uploadedFile);
    finfo_close($finfo);

    if (!$mimeType || $mimeType !== 'application/pdf') {
        respond(400, ['success' => false, 'error' => 'Not a valid PDF file.', 'code' => 400]);
    }
    if (!move_uploaded_file($uploadedFile, $pdfPath)) {
        $cleanup(); respond(500, ['success' => false, 'error' => 'Failed to move uploaded file.', 'code' => 500]);
    }
    $source = ['mode' => 'upload', 'filename' => $originalName];
} else {
    // pdf_url mode
    $dl = download_pdf_from_url($pdfUrl, $pdfPath, MAX_DOWNLOAD_BYTES);
    if (!($dl['ok'] ?? false)) {
        $cleanup();
        respond(400, [
            'success' => false,
            'error'   => 'Failed to fetch pdf_url: ' . ($dl['error'] ?? 'unknown'),
            'code'    => (int)($dl['http'] ?? 400),
        ]);
    }
    $mimeType = 'application/pdf';
    $source   = ['mode' => 'url', 'url' => $pdfUrl];
}

// --- Convert to text (with OCR fallback for image-only pages) ---
$cmd = "pdftotext -layout " . escapeshellarg($pdfPath) . " " . escapeshellarg($textTmp) . " 2>&1";
$output = []; $returnVar = 0;
exec($cmd, $output, $returnVar);

$initialText = (is_file($textTmp) ? (string)file_get_contents($textTmp) : '');

// Decide if OCR is needed
$needOcr = false;
if (!has_cmd('ocrmypdf')) {
    $needOcr = false;
} else {
    if (has_cmd('pdfinfo') && has_cmd('pdftotext')) {
        $pages = pdf_page_count($pdfPath);
        if ($pages && $pages > 0) {
            for ($p = 1; $p <= $pages; $p++) {
                if (!page_has_text($pdfPath, $p)) $ocrPages[] = $p;
            }
            $needOcr = count($ocrPages) > 0;
        } else {
            $needOcr = (trim($initialText) === '');
        }
    } else {
        $needOcr = (trim($initialText) === '');
    }
}

if ($needOcr) {
    $ocrCmd = 'ocrmypdf -l eng --skip-text --optimize 0 '
            . escapeshellarg($pdfPath) . ' ' . escapeshellarg($ocrPdf) . ' 2>&1';
    $ocrOut = []; $ocrRet = 0;
    exec($ocrCmd, $ocrOut, $ocrRet);

    if ($ocrRet === 0 && is_file($ocrPdf)) {
        $ocrUsed = true;
        $reCmd = "pdftotext -layout " . escapeshellarg($ocrPdf) . " " . escapeshellarg($textFinal) . " 2>&1";
        $reOut = []; $reRet = 0;
        exec($reCmd, $reOut, $reRet);
        if ($reRet !== 0 || !is_file($textFinal)) {
            if ($initialText !== '') file_put_contents($textFinal, $initialText);
        }
    } else {
        if ($initialText !== '') {
            file_put_contents($textFinal, $initialText);
        } else {
            @unlink($textTmp); @unlink($ocrPdf); $cleanup();
            respond(500, [
                'success' => false,
                'error'   => 'PDF conversion failed (OCR error).',
                'code'    => 500,
                'details' => ['ocr_ret' => $ocrRet, 'ocr_out' => implode("\n", $ocrOut)]
            ]);
        }
    }
} else {
    if ($returnVar !== 0 || !is_file($textTmp)) {
        $cleanup();
        respond(500, [
            'success' => false,
            'error'   => 'PDF conversion failed.',
            'code'    => 500,
            'details' => ['return' => $returnVar, 'output' => implode("\n", $output)]
        ]);
    }
    rename($textTmp, $textFinal);
}

// --- Read and return text ---
$text = file_get_contents($textFinal);

// cleanup
@unlink($textTmp);
@unlink($ocrPdf);
$cleanup();

respond(200, [
    'success'   => true,
    'text'      => $text,
    'mime'      => $mimeType,
    'source'    => $source,
    'ocr_used'  => $ocrUsed,
    'ocr_pages' => $ocrPages,
]);

*/