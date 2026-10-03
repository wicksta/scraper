#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const execFileAsync = promisify(execFile);
const DEFAULT_SOURCE_DIR = "/mnt/ngist/public_html/newmark/Planning Case Studies";
const DEFAULT_CSV = "/mnt/ngist/public_html/newmark/case_studies.csv";
const DEFAULT_OUTPUT_DIR = "/mnt/ngist/public_html/newmark/case-study-images";
const IMAGE_COLUMN = "Image file";
const REFERENCE_IMAGE_NAMES = new Set([
  "geraldeve.jpeg", "geraldeve.jpg",
  "header.jpeg", "header.jpg", "header.png",
  "header_small.jpeg", "header_small.jpg", "header_small.png",
  "newmark.jpeg", "newmark.jpg", "newmark.png",
  "newmark_small.jpeg", "newmark_small.jpg", "newmark_small.png",
]);
const KNOWN_REFERENCE_SIGNATURES = [
  { name: "geraldeve.jpeg", width: 2640, height: 1131, bytes: 180582 },
  { name: "header.jpeg", width: 2380, height: 224, bytes: 36537 },
  { name: "header_small.jpeg", width: 1190, height: 112, bytes: 11179 },
  { name: "newmark.jpeg", width: 476, height: 173, bytes: 16681 },
  { name: "newmark_small.png", width: 638, height: 221, bytes: 8728 },
];

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-newmark-case-study-images")
  .option("source-dir", {
    type: "string",
    default: DEFAULT_SOURCE_DIR,
    describe: "Root folder containing the case-study DOCX files.",
  })
  .option("csv", {
    type: "string",
    default: DEFAULT_CSV,
    describe: "Case-studies CSV to update.",
  })
  .option("output-dir", {
    type: "string",
    default: DEFAULT_OUTPUT_DIR,
    describe: "Folder in which extracted images will be saved.",
  })
  .option("reference-dir", {
    type: "string",
    default: DEFAULT_OUTPUT_DIR,
    describe: "Folder containing reference logo/header images used by the exclusion filter.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Maximum CSV rows to process; 0 means all.",
  })
  .option("force", {
    type: "boolean",
    default: false,
    describe: "Re-extract images even where the CSV already has an existing image file.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Save images and atomically update the CSV. Default is a read-only preview.",
  })
  .strict()
  .help()
  .argv;

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function normaliseText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function sha256(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let value = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    const next = text[index + 1];
    if (character === '"' && quoted && next === '"') {
      value += '"';
      index += 1;
    } else if (character === '"') {
      quoted = !quoted;
    } else if (character === "," && !quoted) {
      row.push(value);
      value = "";
    } else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && next === "\n") index += 1;
      row.push(value);
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
      value = "";
    } else {
      value += character;
    }
  }
  row.push(value);
  if (row.some((cell) => cell !== "")) rows.push(row);
  const headings = (rows.shift() || []).map((heading) => heading.replace(/^\uFEFF/, "").trim());
  return {
    headings,
    rows: rows.map((cells) => Object.fromEntries(headings.map((heading, index) => [heading, cells[index] || ""]))),
  };
}

function csvValue(value) {
  const raw = String(value == null ? "" : value);
  return /[",\r\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
}

function serialiseCsv(headings, rows) {
  return `${headings.map(csvValue).join(",")}\n${rows.map((row) => headings.map((heading) => csvValue(row[heading])).join(",")).join("\n")}\n`;
}

function findDocxFiles(rootDir) {
  const files = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(entryPath);
      else if (entry.isFile() && /\.docx$/i.test(entry.name)) files.push(entryPath);
    }
  }
  visit(rootDir);
  return files.sort((first, second) => first.localeCompare(second));
}

async function listZipEntries(docxPath) {
  const { stdout } = await execFileAsync("unzip", ["-Z1", docxPath], { encoding: "utf8", maxBuffer: 2 * 1024 * 1024 });
  return stdout.split(/\r?\n/).map((entry) => entry.trim()).filter(Boolean);
}

async function readZipEntry(docxPath, entry) {
  const { stdout } = await execFileAsync("unzip", ["-p", docxPath, entry], {
    encoding: "buffer",
    maxBuffer: 128 * 1024 * 1024,
  });
  return Buffer.isBuffer(stdout) ? stdout : Buffer.from(stdout);
}

function imageDimensions(buffer, extension) {
  const ext = extension.toLowerCase();
  if (ext === ".png" && buffer.length >= 24 && buffer.readUInt32BE(0) === 0x89504e47) {
    return { width: buffer.readUInt32BE(16), height: buffer.readUInt32BE(20) };
  }
  if (ext === ".gif" && buffer.length >= 10 && buffer.toString("ascii", 0, 3) === "GIF") {
    return { width: buffer.readUInt16LE(6), height: buffer.readUInt16LE(8) };
  }
  if (ext === ".webp" && buffer.length >= 30 && buffer.toString("ascii", 0, 4) === "RIFF" && buffer.toString("ascii", 8, 12) === "WEBP") {
    const format = buffer.toString("ascii", 12, 16);
    if (format === "VP8X") {
      return {
        width: 1 + buffer[24] + (buffer[25] << 8) + (buffer[26] << 16),
        height: 1 + buffer[27] + (buffer[28] << 8) + (buffer[29] << 16),
      };
    }
    if (format === "VP8 " && buffer.length >= 30) {
      return { width: buffer.readUInt16LE(26) & 0x3fff, height: buffer.readUInt16LE(28) & 0x3fff };
    }
  }
  if ((ext === ".jpg" || ext === ".jpeg") && buffer.length >= 4 && buffer[0] === 0xff && buffer[1] === 0xd8) {
    let offset = 2;
    while (offset + 9 < buffer.length) {
      if (buffer[offset] !== 0xff) {
        offset += 1;
        continue;
      }
      while (buffer[offset] === 0xff) offset += 1;
      const marker = buffer[offset];
      offset += 1;
      if (marker === 0xd8 || marker === 0xd9) continue;
      if (offset + 2 > buffer.length) break;
      const segmentLength = buffer.readUInt16BE(offset);
      if (segmentLength < 2 || offset + segmentLength > buffer.length) break;
      const isStartOfFrame = (marker >= 0xc0 && marker <= 0xc3)
        || (marker >= 0xc5 && marker <= 0xc7)
        || (marker >= 0xc9 && marker <= 0xcb)
        || (marker >= 0xcd && marker <= 0xcf);
      if (isStartOfFrame && segmentLength >= 7) {
        return { width: buffer.readUInt16BE(offset + 5), height: buffer.readUInt16BE(offset + 3) };
      }
      offset += segmentLength;
    }
  }
  if (ext === ".svg") {
    const svg = buffer.toString("utf8", 0, Math.min(buffer.length, 8192));
    const root = svg.match(/<svg\b[^>]*>/i)?.[0] || "";
    const width = Number.parseFloat(root.match(/\bwidth=["']([0-9.]+)/i)?.[1] || "0");
    const height = Number.parseFloat(root.match(/\bheight=["']([0-9.]+)/i)?.[1] || "0");
    const viewBox = root.match(/\bviewBox=["']\s*[-0-9.]+\s+[-0-9.]+\s+([0-9.]+)\s+([0-9.]+)/i);
    return {
      width: width || Number.parseFloat(viewBox?.[1] || "0"),
      height: height || Number.parseFloat(viewBox?.[2] || "0"),
    };
  }
  return { width: 0, height: 0 };
}

function isImageEntry(entry) {
  return /^word\/media\/[^/]+\.(?:png|jpe?g|gif|webp|emf)$/i.test(entry);
}

async function convertEmfToPng(buffer) {
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "newmark-emf-"));
  const inputPath = path.join(temporaryDirectory, "input.emf");
  const svgPath = path.join(temporaryDirectory, "intermediate.svg");
  const pngPath = path.join(temporaryDirectory, "output.png");
  try {
    fs.writeFileSync(inputPath, buffer);
    await execFileAsync("emf2svg-conv", ["-p", "-i", inputPath, "-o", svgPath], {
      encoding: "utf8",
      maxBuffer: 8 * 1024 * 1024,
    });
    await execFileAsync("rsvg-convert", ["--unlimited", "-o", pngPath, svgPath], {
      encoding: "utf8",
      maxBuffer: 8 * 1024 * 1024,
    });
    return fs.readFileSync(pngPath);
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

async function loadReferenceSignatures(referenceDir) {
  const signatures = [...KNOWN_REFERENCE_SIGNATURES];
  for (const name of REFERENCE_IMAGE_NAMES) {
    const referencePath = path.join(referenceDir, name);
    if (!fs.existsSync(referencePath)) continue;
    const buffer = fs.readFileSync(referencePath);
    let dimensions = imageDimensions(buffer, path.extname(name));
    if (!dimensions.width || !dimensions.height) dimensions = imageDimensions(buffer, ".png");
    signatures.push({ name, width: dimensions.width, height: dimensions.height, bytes: buffer.length });
  }
  return signatures.filter((signature, index, all) => all.findIndex((candidate) => candidate.width === signature.width
    && candidate.height === signature.height && candidate.bytes === signature.bytes) === index);
}

function isExcludedImage(entry, buffer, dimensions, referenceSignatures) {
  const embeddedName = path.basename(entry).toLowerCase();
  if (REFERENCE_IMAGE_NAMES.has(embeddedName)) return true;
  return referenceSignatures.some((reference) => (
    (dimensions.width > 0 && dimensions.height > 0
      && dimensions.width === reference.width && dimensions.height === reference.height)
    || buffer.length === reference.bytes
  ));
}

async function chooseLargestImage(docxPath, referenceSignatures) {
  const entries = (await listZipEntries(docxPath)).filter(isImageEntry);
  let best = null;
  const excluded = [];
  for (const entry of entries) {
    const sourceBuffer = await readZipEntry(docxPath, entry);
    const sourceExtension = path.extname(entry).toLowerCase();
    const buffer = sourceExtension === ".emf" ? await convertEmfToPng(sourceBuffer) : sourceBuffer;
    const extension = sourceExtension === ".emf" ? ".png" : sourceExtension;
    const dimensions = imageDimensions(buffer, extension);
    if (isExcludedImage(entry, sourceBuffer, dimensions, referenceSignatures)) {
      excluded.push({ entry, width: dimensions.width, height: dimensions.height, bytes: sourceBuffer.length });
      continue;
    }
    const area = dimensions.width * dimensions.height;
    if (!best || area > best.area || (area === best.area && sourceBuffer.length > best.sourceBytes)) {
      best = { entry, extension, buffer, sourceBytes: sourceBuffer.length, ...dimensions, area };
    }
  }
  return { image: best, excluded };
}

function slugify(value) {
  const slug = normaliseText(value)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
  return slug || "case-study";
}

function uniqueFilename(base, extension, usedNames) {
  let filename = `${base}${extension}`;
  let suffix = 2;
  while (usedNames.has(filename.toLowerCase())) {
    filename = `${base}-${suffix}${extension}`;
    suffix += 1;
  }
  usedNames.add(filename.toLowerCase());
  return filename;
}

function writeCsvAtomically(csvPath, headings, rows) {
  const tempPath = `${csvPath}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tempPath, serialiseCsv(headings, rows), "utf8");
  fs.renameSync(tempPath, csvPath);
}

function removeObsoleteSvgOutputs(outputDir, rows) {
  const referenced = new Set(rows
    .map((row) => path.basename(normaliseText(row[IMAGE_COLUMN])).toLowerCase())
    .filter(Boolean));
  let removed = 0;
  for (const entry of fs.readdirSync(outputDir, { withFileTypes: true })) {
    if (!entry.isFile() || path.extname(entry.name).toLowerCase() !== ".svg") continue;
    if (referenced.has(entry.name.toLowerCase())) continue;
    const pngPath = path.join(outputDir, `${path.basename(entry.name, path.extname(entry.name))}.png`);
    if (!fs.existsSync(pngPath)) continue;
    fs.unlinkSync(path.join(outputDir, entry.name));
    removed += 1;
  }
  return removed;
}

function findMatchingDocx(row, sourceByName) {
  const fileName = path.basename(normaliseText(row["File name"]));
  if (!fileName) return null;
  const exact = sourceByName.get(fileName.toLowerCase());
  if (exact) return exact;
  const stem = fileName.replace(/\.[^.]+$/, "");
  return sourceByName.get(`${stem}.docx`.toLowerCase()) || null;
}

async function main() {
  const sourceDir = path.resolve(String(argv["source-dir"]));
  const csvPath = path.resolve(String(argv.csv));
  const outputDir = path.resolve(String(argv["output-dir"]));
  const referenceDir = path.resolve(String(argv["reference-dir"]));
  if (!fs.existsSync(sourceDir)) throw new Error(`source folder not found: ${sourceDir}`);
  if (!fs.existsSync(csvPath)) throw new Error(`CSV not found: ${csvPath}`);

  const { headings: originalHeadings, rows } = parseCsv(fs.readFileSync(csvPath, "utf8"));
  const headings = [...originalHeadings];
  if (!headings.includes(IMAGE_COLUMN)) headings.push(IMAGE_COLUMN);
  const sourceFiles = findDocxFiles(sourceDir);
  const sourceByName = new Map(sourceFiles.map((filePath) => [path.basename(filePath).toLowerCase(), filePath]));
  const usedNames = new Set(rows.map((row) => normaliseText(row[IMAGE_COLUMN]).toLowerCase()).filter(Boolean));
  const limit = Math.max(0, Number(argv.limit) || 0);
  const work = limit ? rows.slice(0, limit) : rows;
  const selectedImages = new Map();
  const referenceSignatures = await loadReferenceSignatures(referenceDir);

  logEvent("scan_started", {
    source_dir: sourceDir,
    csv: csvPath,
    output_dir: outputDir,
    reference_dir: referenceDir,
    reference_signatures: referenceSignatures,
    rows: rows.length,
    docx_files: sourceFiles.length,
    queued: work.length,
    apply: argv.apply,
  });

  if (argv.apply) fs.mkdirSync(outputDir, { recursive: true });
  let changed = 0;
  for (const row of work) {
    const docxPath = findMatchingDocx(row, sourceByName);
    const displayName = normaliseText(row.Title || row.Site || row.Name || row["File name"]);
    if (!docxPath) {
      logEvent("missing_docx", { name: displayName, source_file: row["File name"] || "" });
      continue;
    }
    const existingFilename = normaliseText(row[IMAGE_COLUMN]);
    const existingPath = existingFilename ? path.join(outputDir, path.basename(existingFilename)) : "";
    if (!argv.force && existingFilename && fs.existsSync(existingPath)) {
      logEvent("skip_current", { name: displayName, image: existingFilename });
      continue;
    }
    try {
      const cacheKey = docxPath.toLowerCase();
      let selection = selectedImages.get(cacheKey);
      if (!selection) {
        selection = await chooseLargestImage(docxPath, referenceSignatures);
        selectedImages.set(cacheKey, selection);
      }
      if (selection.excluded.length) {
        logEvent("excluded_embedded_images", { name: displayName, source: path.relative(sourceDir, docxPath), images: selection.excluded });
      }
      const image = selection.image;
      if (!image) {
        logEvent("no_embedded_image", { name: displayName, source: path.relative(sourceDir, docxPath) });
        continue;
      }
      const filename = existingFilename && argv.force
        ? `${path.basename(existingFilename, path.extname(existingFilename))}${image.extension}`
        : uniqueFilename(slugify(row.Title || row.Site || row.Name || path.basename(docxPath, ".docx")), image.extension, usedNames);
      const relativeSource = path.relative(sourceDir, docxPath);
      if (!argv.apply) {
        logEvent("would_extract", { name: displayName, source: relativeSource, image: filename, width: image.width, height: image.height, bytes: image.buffer.length });
        continue;
      }
      const outputPath = path.join(outputDir, filename);
      fs.writeFileSync(outputPath, image.buffer);
      if (existingFilename && path.extname(existingFilename).toLowerCase() === ".svg") {
        const previousPath = path.join(outputDir, path.basename(existingFilename));
        if (previousPath !== outputPath && fs.existsSync(previousPath)) fs.unlinkSync(previousPath);
      }
      row[IMAGE_COLUMN] = filename;
      writeCsvAtomically(csvPath, headings, rows);
      changed += 1;
      logEvent("extracted", { name: displayName, source: relativeSource, image: filename, width: image.width, height: image.height, bytes: image.buffer.length, sha256: sha256(image.buffer) });
    } catch (error) {
      logEvent("error", { name: displayName, source: path.relative(sourceDir, docxPath), error: error instanceof Error ? error.message : String(error) });
    }
  }
  const removedSvg = argv.apply ? removeObsoleteSvgOutputs(outputDir, rows) : 0;
  logEvent("complete", { changed, removed_svg: removedSvg, csv: csvPath, output_dir: outputDir });
}

main().catch((error) => {
  logEvent("fatal", { error: error instanceof Error ? error.message : String(error) });
  process.exitCode = 1;
});
