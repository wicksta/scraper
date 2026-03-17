#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once '/var/www/html/docling_job_lib.php';
require_once '/opt/scraper/vendor/autoload.php';
require_once __DIR__ . '/document_render/letters_renderer.php';
require_once __DIR__ . '/document_render/notes_renderer.php';
require_once __DIR__ . '/document_render/minutes_renderer.php';

const DOCRENDER_TEMPLATE_DIR = '/opt/scraper/workers/templates';
const DOCRENDER_OUTPUT_DIR = '/opt/scraper/tmp/generated_documents';

function docrender_parse_cli(array $argv): array
{
    $args = [];
    foreach (array_slice($argv, 1) as $arg) {
        if (preg_match('~^--([a-z0-9_-]+)=(.*)$~i', $arg, $m)) {
            $args[strtolower($m[1])] = $m[2];
        }
    }

    return [
        'request_json_path' => trim((string) ($args['request_json_path'] ?? '')),
    ];
}

function docrender_fail(string $message, int $code = 1): never
{
    fwrite(STDERR, $message . PHP_EOL);
    exit($code);
}

function docrender_require_file(string $path, string $message): void
{
    if ($path === '' || !is_file($path) || !is_readable($path)) {
        throw new RuntimeException($message);
    }
}

function docrender_template_path(string $docType): string
{
    return match ($docType) {
        'letter' => DOCRENDER_TEMPLATE_DIR . '/letter_template.docx',
        'note' => DOCRENDER_TEMPLATE_DIR . '/note_template.docx',
        'minutes' => DOCRENDER_TEMPLATE_DIR . '/minutes_template.docx',
        default => throw new InvalidArgumentException('Unsupported document type.'),
    };
}

function docrender_safe_filename_part(string $value, string $fallback = 'Document'): string
{
    $text = trim(preg_replace('/\s+/u', ' ', $value) ?? $value);
    $text = preg_replace('/[\\x00-\\x1F\\x7F]+/u', '', $text) ?? $text;
    $text = preg_replace('/[\\\\\\/:"*?<>|]+/u', ' ', $text) ?? $text;
    $text = trim(preg_replace('/\s+/u', ' ', $text) ?? $text, " .-\t\n\r\0\x0B");
    if ($text === '') {
        return $fallback;
    }
    return mb_substr($text, 0, 140);
}

function docrender_doc_label(string $docType): string
{
    return match ($docType) {
        'minutes' => 'Minutes',
        'letter' => 'Letter',
        'note' => 'Note',
        default => 'Document',
    };
}

function docrender_title_part(string $docType, array $result): string
{
    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
    return match ($docType) {
        'minutes' => (string) ($meta['title'] ?? $meta['project'] ?? ''),
        'note' => (string) ($meta['title'] ?? $meta['project'] ?? ''),
        'letter' => (string) ($meta['heading'] ?? $meta['address'] ?? ''),
        default => '',
    };
}

function docrender_target_filename(string $docType, array $result): string
{
    $date = date('ymd');
    $label = docrender_doc_label($docType);
    $title = docrender_safe_filename_part(docrender_title_part($docType, $result), $label);
    return sprintf('%s - %s - %s.docx', $date, $label, $title);
}

function docrender_unique_path(string $dir, string $filename): string
{
    $base = pathinfo($filename, PATHINFO_FILENAME);
    $ext = pathinfo($filename, PATHINFO_EXTENSION);
    $candidate = rtrim($dir, '/') . '/' . $filename;
    if (!file_exists($candidate)) {
        return $candidate;
    }

    for ($i = 2; $i < 1000; $i++) {
        $next = rtrim($dir, '/') . '/' . sprintf('%s %d.%s', $base, $i, $ext);
        if (!file_exists($next)) {
            return $next;
        }
    }

    return rtrim($dir, '/') . '/' . sprintf('%s %s.%s', $base, date('His'), $ext);
}

if (PHP_SAPI !== 'cli') {
    http_response_code(405);
    echo "CLI only";
    exit(2);
}

$args = docrender_parse_cli($argv);
$requestJsonPath = $args['request_json_path'];

try {
    docrender_require_file($requestJsonPath, 'Render request JSON was not found.');
    $payload = json_decode((string) file_get_contents($requestJsonPath), true);
    if (!is_array($payload)) {
        throw new RuntimeException('Render request JSON is invalid.');
    }

    $docType = strtolower(trim((string) ($payload['doc_type'] ?? '')));
    $result = $payload['result'] ?? null;
    if (!in_array($docType, ['note', 'letter', 'minutes'], true)) {
        throw new RuntimeException('Invalid or missing doc_type.');
    }
    if (!is_array($result)) {
        throw new RuntimeException('Missing result object.');
    }

    if (!is_dir(DOCRENDER_OUTPUT_DIR) && !mkdir(DOCRENDER_OUTPUT_DIR, 0775, true) && !is_dir(DOCRENDER_OUTPUT_DIR)) {
        throw new RuntimeException('Could not create render output directory.');
    }

    $commonOptions = [
        'template_path' => docrender_template_path($docType),
        'output_dir' => DOCRENDER_OUTPUT_DIR,
        'download_base' => '',
    ];

    $rendered = match ($docType) {
        'letter' => letter_renderer_render_word($result, docling_pdo(), $commonOptions),
        'note' => note_renderer_render_word($result, $commonOptions),
        'minutes' => minutes_renderer_render_word($result, $commonOptions),
    };

    $renderedPath = (string) ($rendered['path'] ?? '');
    docrender_require_file($renderedPath, 'Rendered DOCX was not created.');
    $targetPath = docrender_unique_path(DOCRENDER_OUTPUT_DIR, docrender_target_filename($docType, $result));
    if ($targetPath !== $renderedPath) {
        if (!@rename($renderedPath, $targetPath)) {
            throw new RuntimeException('Failed to rename rendered DOCX.');
        }
        $renderedPath = $targetPath;
    }

    echo json_encode([
        'success' => true,
        'doc_type' => $docType,
        'path' => $renderedPath,
        'file' => basename($renderedPath),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL;
    exit(0);
} catch (Throwable $e) {
    docrender_fail($e->getMessage());
}
