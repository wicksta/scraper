#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';

function parseArgs(argv) {
  const args = {};
  for (const arg of argv.slice(2)) {
    const match = arg.match(/^--([a-z0-9_-]+)=(.*)$/i);
    if (match) {
      args[match[1]] = match[2];
    }
  }
  return {
    input: (args.input || '').trim(),
    title: (args.title || '').trim(),
    fileReference: (args['file-reference'] || '').trim(),
    requestJsonPath: (args['request-json-path'] || '').trim(),
    render: String(args.render || '').trim().toLowerCase() === '1' || String(args.render || '').trim().toLowerCase() === 'true',
  };
}

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function cleanText(value) {
  return String(value || '')
    .replace(/\r\n?/g, '\n')
    .replace(/\u2028|\u2029/g, '\n')
    .replace(/[^\S\n]+/g, ' ')
    .trim();
}

function extractTitle(markdown) {
  const match = markdown.match(/^#\s+(.+)$/m);
  return cleanText(match ? match[1] : '');
}

function flushParagraph(buffer, blocks) {
  const text = cleanText(buffer.join('\n'));
  if (text) {
    blocks.push({ type: 'paragraph', text });
  }
  buffer.length = 0;
}

function flushTable(tableLines, blocks) {
  if (tableLines.length < 2) {
    tableLines.length = 0;
    return;
  }

  const parsedRows = tableLines
    .filter((line, idx) => !(idx === 1 && /^\|\s*[-:| ]+\|\s*$/.test(line.trim())))
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const inner = line.replace(/^\|/, '').replace(/\|$/, '');
      return inner.split('|').map((cell) => cleanText(cell));
    });

  if (parsedRows.length === 0) {
    tableLines.length = 0;
    return;
  }

  const [header, ...rows] = parsedRows;
  blocks.push({
    type: 'table',
    header,
    rows,
  });

  tableLines.length = 0;
}

function markdownToContentBlocks(markdown) {
  const lines = markdown.replace(/\r\n?/g, '\n').split('\n');
  const blocks = [];
  const paragraphBuffer = [];
  const tableLines = [];

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    const trimmed = line.trim();

    if (tableLines.length > 0) {
      if (trimmed.startsWith('|')) {
        tableLines.push(trimmed);
        continue;
      }
      flushTable(tableLines, blocks);
    }

    if (trimmed === '') {
      flushParagraph(paragraphBuffer, blocks);
      continue;
    }

    if (trimmed.startsWith('|')) {
      flushParagraph(paragraphBuffer, blocks);
      tableLines.push(trimmed);
      continue;
    }

    const headingMatch = trimmed.match(/^(#{2,6})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph(paragraphBuffer, blocks);
      const level = headingMatch[1].length;
      const headingText = cleanText(headingMatch[2]);
      if (headingText) {
        blocks.push({
          type: level === 2 ? 'heading' : 'subheading',
          text: headingText,
        });
      }
      continue;
    }

    if (/^\*\*(.+)\*\*$/.test(trimmed)) {
      flushParagraph(paragraphBuffer, blocks);
      const text = cleanText(trimmed.replace(/^\*\*|\*\*$/g, ''));
      if (text) {
        blocks.push({ type: 'subheading', text });
      }
      continue;
    }

    const orderedMatch = trimmed.match(/^(\d+)\.\s+(.+)$/);
    if (orderedMatch) {
      flushParagraph(paragraphBuffer, blocks);
      const text = cleanText(orderedMatch[2]);
      if (text) {
        blocks.push({ type: 'alpha_paragraph', text });
      }
      continue;
    }

    const bulletMatch = trimmed.match(/^-\s+(.+)$/);
    if (bulletMatch) {
      flushParagraph(paragraphBuffer, blocks);
      const text = cleanText(bulletMatch[1]);
      if (text) {
        blocks.push({ type: 'alpha_paragraph', text });
      }
      continue;
    }

    if (/^#\s+/.test(trimmed) || /^Date:\s+/i.test(trimmed)) {
      flushParagraph(paragraphBuffer, blocks);
      continue;
    }

    paragraphBuffer.push(trimmed);
  }

  if (tableLines.length > 0) {
    flushTable(tableLines, blocks);
  }
  flushParagraph(paragraphBuffer, blocks);

  return blocks;
}

function main() {
  const args = parseArgs(process.argv);
  if (!args.input) {
    fail('Missing --input');
  }

  const inputPath = path.resolve(args.input);
  if (!fs.existsSync(inputPath)) {
    fail(`Input Markdown file not found: ${inputPath}`);
  }

  const markdown = fs.readFileSync(inputPath, 'utf8');
  const title = args.title || extractTitle(markdown) || 'Briefing Note';
  const requestJsonPath = path.resolve(
    args.requestJsonPath || path.join(path.dirname(inputPath), `${path.basename(inputPath, path.extname(inputPath))}_note_render_request.json`)
  );

  const payload = {
    doc_type: 'note',
    result: {
      meta: {
        title,
        project: title,
        file_reference: args.fileReference || 'AI retrofit LBC review',
      },
      content_blocks: markdownToContentBlocks(markdown),
    },
  };

  fs.writeFileSync(requestJsonPath, JSON.stringify(payload, null, 2));
  process.stdout.write(`${requestJsonPath}\n`);

  if (!args.render) {
    return;
  }

  const renderResult = spawnSync(
    'php',
    [
      '/opt/scraper/workers/document_render_word.php',
      `--request_json_path=${requestJsonPath}`,
    ],
    {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );

  if (renderResult.status !== 0) {
    const stderr = cleanText(renderResult.stderr || renderResult.stdout || 'DOCX render failed.');
    fail(stderr);
  }

  process.stdout.write(renderResult.stdout || '');
}

main();
