#!/usr/bin/env node

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .scriptName('idox-unified')
  .option('ref', {
    type: 'string',
    describe: 'Application reference',
    demandOption: true,
  })
  .option('start-url', {
    type: 'string',
    describe: 'Idox advanced search URL',
    demandOption: true,
  })
  .option('headed', {
    type: 'boolean',
    describe: 'Run with browser visible',
    default: false,
  })
  .option('output', {
    type: 'string',
    describe: 'Optional output file path for the unified JSON',
    demandOption: false,
  })
  .option('verbose', {
    type: 'boolean',
    describe: 'Print scraper stdout/stderr while running',
    default: false,
  })
  .strict()
  .help()
  .argv;

function runScraper({ ref, startUrl, headed }) {
  return new Promise((resolve) => {
    const args = ['scraper.cjs', '--ref', ref, '--start-url', startUrl];
    if (headed) args.push('--headed');

    const child = spawn(process.execPath, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      stdout += text;
      if (argv.verbose) {
        text
          .split(/\r?\n/)
          .filter(Boolean)
          .forEach((line) => console.log(`[scraper] ${line}`));
      }
    });

    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderr += text;
      if (argv.verbose) {
        text
          .split(/\r?\n/)
          .filter(Boolean)
          .forEach((line) => console.error(`[scraper:err] ${line}`));
      }
    });

    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

function extractUnifiedPath(stdout) {
  const match = stdout.match(/Unified JSON:\s+([^\s]+_UNIFIED\.json)/);
  return match ? match[1] : null;
}

(async () => {
  const result = await runScraper({
    ref: argv.ref,
    startUrl: argv['start-url'],
    headed: argv.headed,
  });

  if (result.code !== 0) {
    if (result.stdout) process.stderr.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    process.exit(result.code || 1);
  }

  const unifiedRelPath = extractUnifiedPath(result.stdout);
  if (!unifiedRelPath) {
    process.stderr.write('Failed to locate Unified JSON output path from scraper stdout.\n');
    process.exit(1);
  }

  const unifiedAbsPath = path.resolve(process.cwd(), unifiedRelPath);
  let unified;
  try {
    unified = JSON.parse(fs.readFileSync(unifiedAbsPath, 'utf8'));
  } catch (err) {
    process.stderr.write(`Failed to read unified output at ${unifiedAbsPath}: ${err.message}\n`);
    process.exit(1);
  }

  if (argv.output) {
    const outputPath = path.isAbsolute(argv.output)
      ? argv.output
      : path.resolve(process.cwd(), argv.output);
    fs.writeFileSync(outputPath, JSON.stringify(unified, null, 2), 'utf8');
  }

  process.stdout.write(`${JSON.stringify(unified, null, 2)}\n`);
})();
