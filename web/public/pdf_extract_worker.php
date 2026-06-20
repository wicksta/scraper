#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * pdf_extract_worker.php
 *
 * Processes queued rows in extract_jobs:
 *  - status=queued → status=running → status=completed/failed
 *  - runs pdftotext, optionally ocrmypdf
 *
 * Usage:
 *   php pdf_extract_worker.php          # process a single job (good for cron)
 *   php pdf_extract_worker.php --loop   # run forever, polling for jobs
 */

// ---- Bootstrap DB ----------------------------------------------------------

function extractor_worker_pg_pdo(): PDO
{
    static $pg = null;
    if ($pg instanceof PDO) {
        return $pg;
    }

    $legacyPgPhp = '/var/www/private/pg.php';
    $port = '5432';
    $db = 'docs_db';
    $user = 'webapp';
    $pass = '';

    if (is_file($legacyPgPhp)) {
        require $legacyPgPhp;
        if (isset($PG_PORT) && $PG_PORT !== '') {
            $port = (string) $PG_PORT;
        }
        if (isset($PG_DB) && $PG_DB !== '') {
            $db = (string) $PG_DB;
        }
        if (isset($PG_USER) && $PG_USER !== '') {
            $user = (string) $PG_USER;
        }
        if (isset($PG_PASSWORD) && $PG_PASSWORD !== '') {
            $pass = (string) $PG_PASSWORD;
        }
    }

    $dsn = sprintf('pgsql:host=%s;port=%s;dbname=%s;sslmode=disable', '127.0.0.1', $port, $db);
    $pg = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ]);
    return $pg;
}

try {
    $pg = extractor_worker_pg_pdo();
} catch (Throwable $e) {
    fwrite(STDERR, "Postgres connection failed: " . $e->getMessage() . "\n");
    exit(1);
}

$pg->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// ---- Basic logging ---------------------------------------------------------

const WORKER_LOG = '/var/log/pdf-extractor-worker.log';

function wlog(string $msg, array $ctx = []): void {
    $line = sprintf(
        "[%s] %s %s\n",
        gmdate('c'),
        $msg,
        $ctx ? json_encode($ctx, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES) : ''
    );
    @file_put_contents(WORKER_LOG, $line, FILE_APPEND);
    fwrite(STDERR, $line);
}

// ---- Helpers copied / adapted from HTTP script -----------------------------

function has_cmd(string $cmd): bool {
    $out = []; $ret = 0;
    @exec('command -v ' . escapeshellarg($cmd) . ' 2>/dev/null', $out, $ret);
    return $ret === 0 && !empty($out);
}

function pdf_page_count(string $pdfPath): ?int {
    $cmd = 'pdfinfo ' . escapeshellarg($pdfPath) . ' 2>/dev/null';
    $out = []; $ret = 0;
    exec($cmd, $out, $ret);
    if ($ret !== 0) return null;
    foreach ($out as $line) {
        if (stripos($line, 'Pages:') === 0) {
            return (int)trim(substr($line, 6));
        }
    }
    return null;
}

function page_has_text(string $pdfPath, int $page): bool {
    $cmd = 'pdftotext -layout -f ' . (int)$page . ' -l ' . (int)$page . ' ' .
           escapeshellarg($pdfPath) . ' - 2>/dev/null';
    $out = []; $ret = 0;
    exec($cmd, $out, $ret);
    $txt = trim(implode("\n", $out));
    return ($ret === 0) && ($txt !== '');
}

// ---- Job handling ----------------------------------------------------------

/**
 * Atomically claim the next queued job (if any).
 * Returns associative array or null.
 */
function claim_next_job(PDO $pg): ?array
{
    // Postgres-specific: SELECT ... FOR UPDATE SKIP LOCKED inside a CTE
    $sql = <<<SQL
WITH job AS (
  SELECT id
  FROM extract_jobs
  WHERE status = 'queued'
  ORDER BY created_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE extract_jobs e
SET status = 'running',
    progress = 10,
    updated_at = now()
FROM job
WHERE e.id = job.id
RETURNING e.*;
SQL;

    $stmt = $pg->query($sql);
    $job = $stmt->fetch(PDO::FETCH_ASSOC);
    if (!$job) {
        return null;
    }
    wlog('claimed_job', ['id' => $job['id']]);
    return $job;
}

function update_job(PDO $pg, int $id, array $fields): void
{
    if (!$fields) return;

    $sets = [];
    $params = [':id' => $id];
    foreach ($fields as $k => $v) {
        $param = ':' . $k;
        if ($k === 'ocr_pages') {
            $sets[] = $k . ' = ' . $param . '::integer[]';
            $params[$param] = worker_pg_int_array_literal($v);
            continue;
        }
        $sets[] = $k . ' = ' . $param;
        $params[$param] = $v;
    }
    $sets[] = 'updated_at = now()';

    $sql = 'UPDATE extract_jobs SET ' . implode(', ', $sets) . ' WHERE id = :id';
    $stmt = $pg->prepare($sql);
    $stmt->execute($params);
}

function worker_pg_int_array_literal($value): ?string
{
    if ($value === null) {
        return null;
    }

    $items = is_array($value) ? $value : [];
    if (!$items) {
        return '{}';
    }

    $ints = array_map(static fn($v): int => (int)$v, $items);
    return '{' . implode(',', $ints) . '}';
}

function mark_failed(PDO $pg, int $id, string $message): void
{
    wlog('job_failed', ['id' => $id, 'error' => $message]);
    update_job($pg, $id, [
        'status'        => 'failed',
        'progress'      => 100,
        'error_message' => $message,
    ]);
}

/**
 * Core extraction logic for a single job.
 */
function process_job(PDO $pg, array $job): void
{
    $jobId   = (int)$job['id'];
    $pdfPath = $job['pdf_path'];

    if (!is_file($pdfPath)) {
        mark_failed($pg, $jobId, "PDF file missing at {$pdfPath}");
        return;
    }

    wlog('job_processing_start', ['id' => $jobId, 'pdfPath' => $pdfPath]);

    $textTmp   = $pdfPath . '.tmp.txt';
    $textFinal = $pdfPath . '.txt';
    $ocrUsed   = false;
    $ocrPages  = [];

    // ---- Initial pdftotext on whole document --------------------------------

    update_job($pg, $jobId, ['progress' => 20]);

    $cmd = "pdftotext -layout " . escapeshellarg($pdfPath) . " " . escapeshellarg($textTmp) . " 2>&1";
    $output = []; $ret = 0;
    exec($cmd, $output, $ret);

    $initialText = (is_file($textTmp) ? (string)file_get_contents($textTmp) : '');
    wlog('pdftotext_done', [
        'job_id' => $jobId,
        'ret'    => $ret,
        'bytes'  => strlen($initialText),
    ]);

    // ---- Decide if OCR is needed --------------------------------------------

    $needOcr = false;

    if (!has_cmd('ocrmypdf')) {
        wlog('ocr_unavailable', ['job_id' => $jobId]);
    } else {
        // Prefer detailed probe if pdfinfo/pdftotext are available
        if (has_cmd('pdfinfo') && has_cmd('pdftotext')) {
            $pages = pdf_page_count($pdfPath);
            if ($pages && $pages > 0) {
                $probeStart = microtime(true);
                // You could cap to first N pages if you like
                for ($p = 1; $p <= $pages; $p++) {
                    if (!page_has_text($pdfPath, $p)) {
                        $ocrPages[] = $p;
                    }
                }
                $needOcr = count($ocrPages) > 0;
                wlog('probe_pages', [
                    'job_id'    => $jobId,
                    'pages'     => $pages,
                    'ocr_pages' => $ocrPages,
                    'dur_s'     => round(microtime(true) - $probeStart, 3),
                ]);
            } else {
                $needOcr = (trim($initialText) === '');
                wlog('probe_unknown_pages', [
                    'job_id'            => $jobId,
                    'needOcrHeuristic'  => $needOcr,
                ]);
            }
        } else {
            $needOcr = (trim($initialText) === '');
            wlog('probe_tools_missing', [
                'job_id'            => $jobId,
                'needOcrHeuristic'  => $needOcr,
            ]);
        }
    }

    // ---- OCR path -----------------------------------------------------------

    $text = '';
    if ($needOcr) {
        update_job($pg, $jobId, ['progress' => 40]);
        $ocrStart = microtime(true);
        $ocrPdf = $pdfPath . '.ocr.pdf';
        $ocrCmd = 'ocrmypdf -l eng --skip-text --optimize 0 ' .
                  escapeshellarg($pdfPath) . ' ' . escapeshellarg($ocrPdf) . ' 2>&1';
        $ocrOut = []; $ocrRet = 0;
        exec($ocrCmd, $ocrOut, $ocrRet);
        wlog('ocr_done', [
            'job_id' => $jobId,
            'ret'    => $ocrRet,
            'dur_s'  => round(microtime(true) - $ocrStart, 3),
        ]);

        if ($ocrRet === 0 && is_file($ocrPdf)) {
            $ocrUsed = true;
            update_job($pg, $jobId, ['progress' => 70]);
            $reCmd = "pdftotext -layout " . escapeshellarg($ocrPdf) . " " . escapeshellarg($textFinal) . " 2>&1";
            $reOut = []; $reRet = 0;
            exec($reCmd, $reOut, $reRet);
            wlog('pdftotext_after_ocr', ['job_id' => $jobId, 'ret' => $reRet]);

            if ($reRet === 0 && is_file($textFinal)) {
                $text = (string)file_get_contents($textFinal);
            } elseif ($initialText !== '') {
                // Fallback to initial text if we have any
                file_put_contents($textFinal, $initialText);
                $text = $initialText;
            } else {
                @unlink($textTmp);
                @unlink($ocrPdf);
                mark_failed($pg, $jobId, 'PDF conversion failed (OCR error).');
                return;
            }

            @unlink($ocrPdf);
        } else {
            if ($initialText !== '') {
                // Fallback: keep initial text
                file_put_contents($textFinal, $initialText);
                $text = $initialText;
            } else {
                @unlink($textTmp);
                @unlink($ocrPdf);
                mark_failed($pg, $jobId, 'PDF conversion failed (OCR error).');
                return;
            }
        }
    } else {
        // ---- No OCR needed: rely on initial pdftotext result ----------------
        if ($ret !== 0 || !is_file($textTmp)) {
            wlog('fatal_pdftotext_failed', [
                'job_id' => $jobId,
                'ret'    => $ret,
                'out'    => implode("\n", $output),
            ]);
            mark_failed($pg, $jobId, 'PDF conversion failed.');
            return;
        }
        rename($textTmp, $textFinal);
        $text = (string)file_get_contents($textFinal);
    }

    // Cleanup temp text files (but keep original PDF if you want)
    @unlink($textTmp);
    @unlink($textFinal); // or keep if you like; optional

    // ---- Store result back to DB -------------------------------------------

    update_job($pg, $jobId, [
        'status'      => 'completed',
        'progress'    => 100,
        'text_result' => $text,
        'ocr_used'    => $ocrUsed ? 1 : 0,
        'ocr_pages'   => $ocrPages,
        'error_message' => null,
    ]);

    wlog('job_completed', [
        'job_id'     => $jobId,
        'bytes_text' => strlen($text),
        'ocr_used'   => $ocrUsed,
    ]);
}

// ---- Main runner -----------------------------------------------------------

$loop = in_array('--loop', $argv, true);
do {
    $job = claim_next_job($pg);
    if ($job) {
        try {
            process_job($pg, $job);
        } catch (Throwable $e) {
            mark_failed($pg, (int)$job['id'], 'Worker exception: ' . $e->getMessage());
        }
    } else {
        if (!$loop) {
            wlog('no_jobs_available');
            break;
        }
        // Looping mode: sleep briefly then check again
        sleep(5);
    }
} while ($loop);

wlog('worker_exit', ['loop' => $loop]);
