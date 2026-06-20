<?php
declare(strict_types=1);

const DOCLING_ENV_FILE = '/opt/scraper/ngist/.env';
const DOCLING_FALLBACK_ENV_FILE = '/opt/scraper/.env';
const DOCLING_JOB_BASE_DIR = '/tmp/docling-pagewise-api';
const DOCLING_DEFAULT_API_KEY = 'bdb2b5a815c6db39cc8f175acfafbbad414c77024e2230f9511fd8dd55ab4d5a';
const DOCLING_JOB_RETENTION_SECONDS = 86400;

function docling_load_env(string $file): array
{
    static $cache = [];
    if (array_key_exists($file, $cache)) {
        return $cache[$file];
    }

    $env = [];
    if (!is_readable($file)) {
        return $cache[$file] = $env;
    }

    $lines = file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
    foreach ($lines as $line) {
        $line = trim($line);
        if ($line === '' || str_starts_with($line, '#')) {
            continue;
        }
        $pos = strpos($line, '=');
        if ($pos === false) {
            continue;
        }
        $key = trim(substr($line, 0, $pos));
        $val = trim(substr($line, $pos + 1));
        if ($key === '') {
            continue;
        }
        if ((str_starts_with($val, '"') && str_ends_with($val, '"')) || (str_starts_with($val, "'") && str_ends_with($val, "'"))) {
            $val = substr($val, 1, -1);
        }
        $env[$key] = $val;
    }

    return $cache[$file] = $env;
}

function docling_env(string $key, ?string $default = null): ?string
{
    $primary = docling_load_env(DOCLING_ENV_FILE);
    if (array_key_exists($key, $primary)) {
        return $primary[$key];
    }

    $fallback = docling_load_env(DOCLING_FALLBACK_ENV_FILE);
    if (array_key_exists($key, $fallback)) {
        return $fallback[$key];
    }

    $runtime = getenv($key);
    if ($runtime !== false && $runtime !== '') {
        return $runtime;
    }
    return $default;
}

function docling_pdo(bool $forceReconnect = false): PDO
{
    static $pdo = null;
    if ($forceReconnect) {
        $pdo = null;
    }
    if ($pdo instanceof PDO) {
        return $pdo;
    }

    $candidates = [
        [
            'host' => (string) docling_env('MYSQL_HOST', ''),
            'db' => (string) docling_env('MYSQL_DB', ''),
            'user' => (string) docling_env('MYSQL_USER', ''),
            'pass' => (string) docling_env('MYSQL_PASS', ''),
            'port' => (string) docling_env('MYSQL_PORT', '3306'),
        ],
        [
            'host' => (string) (docling_load_env(DOCLING_FALLBACK_ENV_FILE)['MYSQL_HOST'] ?? ''),
            'db' => (string) (docling_load_env(DOCLING_FALLBACK_ENV_FILE)['MYSQL_DATABASE'] ?? ''),
            'user' => (string) (docling_load_env(DOCLING_FALLBACK_ENV_FILE)['MYSQL_USER'] ?? ''),
            'pass' => (string) (docling_load_env(DOCLING_FALLBACK_ENV_FILE)['MYSQL_PASSWORD'] ?? ''),
            'port' => (string) (docling_load_env(DOCLING_FALLBACK_ENV_FILE)['MYSQL_PORT'] ?? '3306'),
        ],
    ];

    $lastException = null;
    foreach ($candidates as $cfg) {
        if (($cfg['host'] ?? '') === '' || ($cfg['db'] ?? '') === '' || ($cfg['user'] ?? '') === '') {
            continue;
        }
        $dsn = sprintf('mysql:host=%s;port=%s;dbname=%s;charset=utf8', $cfg['host'], $cfg['port'], $cfg['db']);
        try {
            $pdo = new PDO($dsn, $cfg['user'], $cfg['pass'], [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ]);
            return $pdo;
        } catch (PDOException $e) {
            $lastException = $e;
        }
    }

    if ($lastException instanceof PDOException) {
        throw $lastException;
    }
    throw new PDOException('No usable MySQL configuration found for Docling jobs.');
}

function docling_db_exception_is_retryable(PDOException $e): bool
{
    $message = strtolower($e->getMessage());
    return str_contains($message, 'server has gone away')
        || str_contains($message, 'lost connection')
        || str_contains($message, 'error while sending')
        || str_contains($message, 'error writing communication packets');
}

function docling_db_warning(string $message): void
{
    error_log('[docling-db] ' . $message);
}

function docling_db_run(callable $fn, bool $swallowOnFailure = false): mixed
{
    $attempt = 0;
    while (true) {
        try {
            return $fn(docling_pdo($attempt > 0));
        } catch (PDOException $e) {
            if ($attempt === 0 && docling_db_exception_is_retryable($e)) {
                $attempt += 1;
                docling_db_warning('Retrying after transient MySQL error: ' . $e->getMessage());
                continue;
            }
            if ($swallowOnFailure) {
                docling_db_warning('Non-fatal Docling DB update failure: ' . $e->getMessage());
                return null;
            }
            throw $e;
        }
    }
}

function docling_expected_api_key(): string
{
    return (string) docling_env('PDF_EXTRACT_KEY', DOCLING_DEFAULT_API_KEY);
}

function docling_json_out(int $status, array $payload): void
{
    if (PHP_SAPI !== 'cli') {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE);
    exit;
}

function docling_boolish(mixed $value): bool
{
    if (is_bool($value)) {
        return $value;
    }
    $value = strtolower(trim((string) $value));
    return in_array($value, ['1', 'true', 'yes', 'on'], true);
}

function docling_job_create(array $fields): int
{
    if (!array_key_exists('stage', $fields)) {
        $fields['stage'] = 1;
    }
    if (!array_key_exists('created_at', $fields)) {
        $fields['created_at'] = date('Y-m-d H:i:s');
    }

    return (int) docling_db_run(function (PDO $pdo) use ($fields): int {
        $sql = 'INSERT INTO app_ingest_jobs (' . implode(',', array_keys($fields)) . ') VALUES (' .
            implode(',', array_fill(0, count($fields), '?')) . ')';
        $st = $pdo->prepare($sql);
        $st->execute(array_values($fields));
        return (int) $pdo->lastInsertId();
    });
}

function docling_job_update(int $jobId, array $fields): void
{
    if ($jobId <= 0 || $fields === []) {
        return;
    }

    docling_db_run(function (PDO $pdo) use ($jobId, $fields): void {
        $sets = [];
        $params = [':id' => $jobId];
        foreach ($fields as $key => $value) {
            $sets[] = "`$key` = :$key";
            $params[":$key"] = is_array($value) ? json_encode($value, JSON_UNESCAPED_UNICODE) : $value;
        }
        $sets[] = '`updated_at` = NOW()';
        $sql = 'UPDATE app_ingest_jobs SET ' . implode(', ', $sets) . ' WHERE id = :id';
        $st = $pdo->prepare($sql);
        $st->execute($params);
    }, true);
}

function docling_job_finish(int $jobId, array $payload, string $status = 'completed'): void
{
    docling_db_run(function (PDO $pdo) use ($jobId, $payload, $status): void {
        $st = $pdo->prepare(
            'UPDATE app_ingest_jobs
                SET status = :status,
                    percentage_complete = 100,
                    final_output = :final_output,
                    error_message = NULL,
                    updated_at = NOW()
              WHERE id = :id'
        );
        $st->execute([
            ':status' => $status,
            ':final_output' => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE),
            ':id' => $jobId,
        ]);
    }, true);
}

function docling_job_fail(int $jobId, string $message, ?array $payload = null): void
{
    docling_db_run(function (PDO $pdo) use ($jobId, $message, $payload): void {
        $st = $pdo->prepare(
            'UPDATE app_ingest_jobs
                SET status = :status,
                    error_message = :message,
                    final_output = :final_output,
                    updated_at = NOW()
              WHERE id = :id'
        );
        $st->execute([
            ':status' => 'error',
            ':message' => $message,
            ':final_output' => json_encode($payload ?? ['error' => $message], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_IGNORE),
            ':id' => $jobId,
        ]);
    }, true);
}

function docling_job_get(int $jobId): ?array
{
    $row = docling_db_run(function (PDO $pdo) use ($jobId) {
        $st = $pdo->prepare(
            'SELECT id, kind, stage, status, percentage_complete, current_action, error_message, created_at, updated_at, final_output
               FROM app_ingest_jobs
              WHERE id = :id'
        );
        $st->execute([':id' => $jobId]);
        return $st->fetch();
    });
    if (!$row) {
        return null;
    }
    $row['id'] = (int) $row['id'];
    $row['stage'] = (int) $row['stage'];
    $row['percentage_complete'] = (int) $row['percentage_complete'];
    $row['final_output'] = $row['final_output'] ? json_decode((string) $row['final_output'], true) : null;
    return $row;
}

function docling_job_dir(int $jobId): string
{
    if (!is_dir(DOCLING_JOB_BASE_DIR)) {
        @mkdir(DOCLING_JOB_BASE_DIR, 0777, true);
    }
    @chmod(DOCLING_JOB_BASE_DIR, 0777);

    $dir = DOCLING_JOB_BASE_DIR . '/job_' . $jobId;
    if (!is_dir($dir)) {
        @mkdir($dir, 0777, true);
    }
    @chmod($dir, 0777);
    return $dir;
}

function docling_worker_log(int $jobId, string $message): void
{
    $dir = docling_job_dir($jobId);
    $line = sprintf("[%s] %s\n", gmdate('c'), $message);
    @file_put_contents($dir . '/worker.log', $line, FILE_APPEND);
}

function docling_delete_tree(string $path): void
{
    if (is_link($path) || is_file($path)) {
        @unlink($path);
        return;
    }
    if (!is_dir($path)) {
        return;
    }

    $items = @scandir($path);
    if (!is_array($items)) {
        return;
    }
    foreach ($items as $item) {
        if ($item === '.' || $item === '..') {
            continue;
        }
        docling_delete_tree($path . '/' . $item);
    }
    @rmdir($path);
}

function docling_cleanup_old_job_dirs(int $maxAgeSeconds = DOCLING_JOB_RETENTION_SECONDS): int
{
    static $ran = false;
    if ($ran) {
        return 0;
    }
    $ran = true;

    if (!is_dir(DOCLING_JOB_BASE_DIR)) {
        return 0;
    }

    $deleted = 0;
    $cutoff = time() - max(60, $maxAgeSeconds);
    $dirs = glob(DOCLING_JOB_BASE_DIR . '/job_*', GLOB_ONLYDIR) ?: [];
    foreach ($dirs as $dir) {
        $mtime = @filemtime($dir);
        if ($mtime === false || $mtime > $cutoff) {
            continue;
        }
        docling_delete_tree($dir);
        if (!file_exists($dir)) {
            $deleted++;
        }
    }

    return $deleted;
}

function docling_request_api_key(): string
{
    return (string) (
        $_POST['api_key']
        ?? $_GET['api_key']
        ?? $_SERVER['HTTP_X_API_KEY']
        ?? ''
    );
}
