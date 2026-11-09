#!/usr/bin/env node
/*
 * Renders the production manuals (deploy-guide.md, user-guide.md) as Word documents.
 *
 *   node build-docx.cjs --out <dir> [--version 1.0.0] [--commit abcd1234] [--images <dir>]
 *
 * Needs the `docx` npm package (deploy/build-prod-package.ps1 installs it and sets NODE_PATH).
 * The Markdown subset understood here is what the two manuals use: # to #### headings, paragraphs
 * with **bold** and `code`, - and 1. lists, pipe tables, fenced code blocks, a paragraph holding one
 * ![caption](images/x.jpg), and > quotes starting with 提示 / 注意. Anything else is an error, so a
 * manual never ships with raw Markdown in it.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const {
  AlignmentType, BorderStyle, Document, Footer, Header, HeadingLevel, ImageRun, LevelFormat, Packer,
  PageBreak, PageNumber, Paragraph, ShadingType, Table, TableCell, TableOfContents, TableRow, TextRun,
  WidthType, VerticalAlign
} = require('docx');

const args = process.argv.slice(2);
const arg = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const outDir = arg('out', path.join(__dirname, 'out'));
const version = arg('version', 'dev');
const commit = arg('commit', '');
const imagesDir = arg('images', path.join(__dirname, 'images'));

const DOCS = [
  { source: 'deploy-guide.md', file: '部署手册.docx', subtitle: 'Docker 单机部署、运维、备份恢复与升级' },
  { source: 'user-guide.md', file: '操作手册.docx', subtitle: '数据源、同步任务、监控与常见情况处理' }
];

// A4, 2.2 cm side margins: 11906 - 2 * 1247 = 9412 DXA of text width.
const PAGE = { width: 11906, height: 16838, margin: 1247 };
const TEXT_WIDTH = PAGE.width - 2 * PAGE.margin;
const FONT = 'Microsoft YaHei';
const MONO = 'Consolas';
const font = name => ({ ascii: name, hAnsi: name, eastAsia: name === MONO ? FONT : name, cs: name });
const COLOR = { accent: '1F4E79', text: '262626', muted: '7F7F7F', border: 'BFBFBF', head: 'DCE6F1', code: 'F2F2F2', note: 'EAF1FB', warn: 'FFF4E5' };

// ---------------------------------------------------------------- inline text

function runs(text, extra = {}) {
  const out = [];
  const pattern = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0;
  let m;
  while ((m = pattern.exec(text))) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...extra }));
    const token = m[0];
    if (token.startsWith('**')) out.push(new TextRun({ text: token.slice(2, -2), bold: true, ...extra }));
    else out.push(new TextRun({ text: token.slice(1, -1), font: font(MONO), size: 19, ...extra }));
    last = m.index + token.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...extra }));
  if (/\*\*|`|!\[|\]\(/.test(text.replace(pattern, ''))) throw new Error(`unsupported Markdown in: ${text}`);
  return out;
}

// Display width for column sizing: CJK characters count double.
const displayWidth = text => [...text.replace(/\*\*|`/g, '')].reduce((n, ch) => n + (/[⺀-￯]/.test(ch) ? 2 : 1), 0);

// ---------------------------------------------------------------- images

function jpegSize(buffer) {
  let i = 2;
  while (i < buffer.length) {
    if (buffer[i] !== 0xff) throw new Error('not a JPEG');
    const marker = buffer[i + 1];
    const length = buffer.readUInt16BE(i + 2);
    if (marker >= 0xc0 && marker <= 0xc3) return { height: buffer.readUInt16BE(i + 5), width: buffer.readUInt16BE(i + 7) };
    i += 2 + length;
  }
  throw new Error('JPEG without a frame header');
}

function imageSize(file, buffer) {
  if (file.endsWith('.png')) return { width: buffer.readUInt32BE(16), height: buffer.readUInt32BE(20) };
  return jpegSize(buffer);
}

// ---------------------------------------------------------------- Markdown blocks

function parse(markdown) {
  const lines = markdown.replace(/\r\n/g, '\n').split('\n');
  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    let m;
    if ((m = /^(#{1,4}) (.+)$/.exec(line))) { blocks.push({ type: 'heading', level: m[1].length, text: m[2].trim() }); i++; continue; }
    if (line.startsWith('```')) {
      const code = [];
      i++;
      while (i < lines.length && !lines[i].startsWith('```')) code.push(lines[i++]);
      if (i >= lines.length) throw new Error('unterminated code block');
      i++;
      blocks.push({ type: 'code', lines: code });
      continue;
    }
    if (line.startsWith('|')) {
      const rows = [];
      while (i < lines.length && lines[i].startsWith('|')) rows.push(lines[i++]);
      const cells = row => row.trim().replace(/^\||\|$/g, '').split('|').map(c => c.trim());
      if (rows.length < 2 || !/^\|[\s:|-]+\|$/.test(rows[1].trim())) throw new Error(`table without a separator row: ${rows[0]}`);
      blocks.push({ type: 'table', header: cells(rows[0]), rows: rows.slice(2).map(cells) });
      continue;
    }
    if ((m = /^!\[([^\]]*)\]\(([^)]+)\)$/.exec(line.trim()))) { blocks.push({ type: 'image', caption: m[1], src: m[2] }); i++; continue; }
    if (line.startsWith('> ')) {
      const text = [];
      while (i < lines.length && lines[i].startsWith('> ')) text.push(lines[i++].slice(2).trim());
      blocks.push({ type: 'note', text: text.join('') });
      continue;
    }
    if (/^(- |\d+\. )/.test(line)) {
      const ordered = /^\d+\. /.test(line);
      const items = [];
      while (i < lines.length && /^(- |\d+\. )/.test(lines[i])) {
        items.push(lines[i].replace(/^(- |\d+\. )/, '').trim());
        i++;
      }
      blocks.push({ type: 'list', ordered, items });
      continue;
    }
    const text = [];
    while (i < lines.length && lines[i].trim() && !/^(#{1,4} |```|\||> |- |\d+\. |!\[)/.test(lines[i])) text.push(lines[i++].trim());
    if (!text.length) throw new Error(`cannot parse line ${i + 1}: ${line}`);
    blocks.push({ type: 'paragraph', text: text.join('') });
  }
  return blocks;
}

// ---------------------------------------------------------------- rendering

const cellBorder = { style: BorderStyle.SINGLE, size: 4, color: COLOR.border };
const borders = { top: cellBorder, bottom: cellBorder, left: cellBorder, right: cellBorder };

function columnWidths(header, rows) {
  const n = header.length;
  const want = header.map((h, c) => Math.max(displayWidth(h), ...rows.map(r => displayWidth(r[c] || ''))));
  // Short columns keep their natural width; long ones share what is left.
  const min = 900;
  const raw = want.map(w => Math.max(min, Math.min(w, 60) * 110));
  const total = raw.reduce((a, b) => a + b, 0);
  const widths = raw.map(w => Math.floor((w / total) * TEXT_WIDTH));
  widths[n - 1] += TEXT_WIDTH - widths.reduce((a, b) => a + b, 0);
  return widths;
}

function renderTable(block) {
  const widths = columnWidths(block.header, block.rows);
  const cell = (text, c, head) =>
    new TableCell({
      width: { size: widths[c], type: WidthType.DXA },
      borders,
      verticalAlign: VerticalAlign.CENTER,
      shading: head ? { type: ShadingType.CLEAR, color: 'auto', fill: COLOR.head } : undefined,
      margins: { top: 60, bottom: 60, left: 100, right: 100 },
      children: [new Paragraph({ spacing: { before: 0, after: 0, line: 300 }, children: runs(text, { size: 19, bold: head || undefined }) })]
    });
  return new Table({
    width: { size: TEXT_WIDTH, type: WidthType.DXA },
    columnWidths: widths,
    rows: [
      new TableRow({ tableHeader: true, children: block.header.map((h, c) => cell(h, c, true)) }),
      ...block.rows.map(r => {
        if (r.length !== block.header.length) throw new Error(`table row has ${r.length} cells, header ${block.header.length}: ${r.join(' | ')}`);
        return new TableRow({ cantSplit: true, children: r.map((t, c) => cell(t, c, false)) });
      })
    ]
  });
}

function render(blocks, state) {
  const out = [];
  for (const block of blocks) {
    switch (block.type) {
      case 'heading': {
        if (block.level === 1) continue; // the document title is on the cover page
        const level = [null, null, HeadingLevel.HEADING_1, HeadingLevel.HEADING_2, HeadingLevel.HEADING_3][block.level];
        if (block.level === 2 && state.chapters++ > 0) out.push(new Paragraph({ children: [new PageBreak()] }));
        out.push(new Paragraph({ heading: level, children: runs(block.text) }));
        break;
      }
      case 'paragraph':
        out.push(new Paragraph({ style: 'body', children: runs(block.text) }));
        break;
      case 'list': {
        const instance = state.lists++;
        for (const item of block.items) {
          out.push(new Paragraph({
            style: 'body',
            numbering: { reference: block.ordered ? 'numbers' : 'bullets', level: 0, instance: block.ordered ? instance : 0 },
            children: runs(item)
          }));
        }
        break;
      }
      case 'table':
        out.push(renderTable(block));
        out.push(new Paragraph({ style: 'body', spacing: { before: 0, after: 60 }, children: [] }));
        break;
      case 'code':
        block.lines.forEach((line, n) => {
          out.push(new Paragraph({
            shading: { type: ShadingType.CLEAR, color: 'auto', fill: COLOR.code },
            spacing: { before: n === 0 ? 120 : 0, after: n === block.lines.length - 1 ? 160 : 0, line: 276 },
            indent: { left: 120, right: 120 },
            keepLines: true,
            keepNext: n < block.lines.length - 1,
            children: [new TextRun({ text: line || ' ', font: font(MONO), size: 18 })]
          }));
        });
        break;
      case 'note': {
        const warn = block.text.startsWith('注意');
        out.push(new Paragraph({
          style: 'body',
          shading: { type: ShadingType.CLEAR, color: 'auto', fill: warn ? COLOR.warn : COLOR.note },
          border: { left: { style: BorderStyle.SINGLE, size: 18, color: warn ? 'E6A23C' : '409EFF', space: 6 } },
          indent: { left: 160, right: 120 },
          spacing: { before: 120, after: 160 },
          children: runs(block.text)
        }));
        break;
      }
      case 'image': {
        const file = path.join(imagesDir, path.basename(block.src));
        if (!fs.existsSync(file)) throw new Error(`missing image ${file}`);
        const data = fs.readFileSync(file);
        const size = imageSize(file, data);
        // At most the text width (in px at 96 dpi) and 60% of the page height.
        const maxWidth = Math.floor((TEXT_WIDTH / 1440) * 96);
        const maxHeight = Math.floor(((PAGE.height - 2 * 1440) / 1440) * 96 * 0.62);
        const scale = Math.min(maxWidth / size.width, maxHeight / size.height, 1);
        state.figures++;
        out.push(new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { before: 160, after: 40 },
          keepNext: true,
          children: [new ImageRun({
            type: file.endsWith('.png') ? 'png' : 'jpg',
            data,
            transformation: { width: Math.round(size.width * scale), height: Math.round(size.height * scale) },
            altText: { title: block.caption, description: block.caption, name: path.basename(file) }
          })]
        }));
        out.push(new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { before: 0, after: 200 },
          children: [new TextRun({ text: `图 ${state.figures}  ${block.caption}`, size: 18, color: COLOR.muted })]
        }));
        break;
      }
      default:
        throw new Error(`unknown block ${block.type}`);
    }
  }
  return out;
}

function cover(title, subtitle) {
  const today = new Date().toISOString().slice(0, 10);
  const line = (text, opts) => new Paragraph({ alignment: AlignmentType.CENTER, ...opts.paragraph, children: [new TextRun({ text, ...opts.run })] });
  return [
    new Paragraph({ spacing: { before: 2600 }, children: [] }),
    line('数据同步平台', { paragraph: { spacing: { after: 240 } }, run: { size: 36, color: COLOR.muted } }),
    line(title, { paragraph: { spacing: { after: 360 } }, run: { size: 64, bold: true, color: COLOR.accent } }),
    line(subtitle, { paragraph: { spacing: { after: 2400 } }, run: { size: 26, color: COLOR.text } }),
    line(`版本 ${version}`, { paragraph: { spacing: { after: 120 } }, run: { size: 24 } }),
    line(`编制日期 ${today}`, { paragraph: { spacing: { after: 120 } }, run: { size: 24 } }),
    ...(commit ? [line(`构建 ${commit}`, { paragraph: {}, run: { size: 20, color: COLOR.muted } })] : []),
    new Paragraph({ children: [new PageBreak()] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 240 }, children: [new TextRun({ text: '目  录', bold: true, size: 32 })] }),
    new TableOfContents('目录', { hyperlink: true, headingStyleRange: '1-3' }),
    new Paragraph({ style: 'body', children: [new TextRun({ text: '（如目录为空，请在 Word 中右键目录 →「更新域」）', size: 18, color: COLOR.muted })] }),
    new Paragraph({ children: [new PageBreak()] })
  ];
}

function build(doc) {
  const markdown = fs.readFileSync(path.join(__dirname, doc.source), 'utf8');
  const blocks = parse(markdown);
  const title = blocks[0] && blocks[0].type === 'heading' && blocks[0].level === 1 ? blocks[0].text.replace(/^数据同步平台\s*/, '') : doc.file;
  const state = { chapters: 0, lists: 1, figures: 0 };
  const body = render(blocks, state);
  const heading = (size, before, after) => ({
    run: { font: font(FONT), size, bold: true, color: COLOR.accent },
    paragraph: { spacing: { before, after }, keepNext: true, keepLines: true }
  });
  const document = new Document({
    creator: '数据同步平台',
    title: `数据同步平台 ${title}`,
    description: `版本 ${version}`,
    features: { updateFields: true },
    styles: {
      default: {
        document: { run: { font: font(FONT), size: 21, color: COLOR.text }, paragraph: { spacing: { line: 320 } } },
        heading1: heading(32, 240, 200),
        heading2: heading(26, 280, 140),
        heading3: heading(23, 220, 100)
      },
      paragraphStyles: [
        { id: 'body', name: 'Body Text', basedOn: 'Normal', quickFormat: true, run: { font: font(FONT), size: 21 }, paragraph: { spacing: { before: 60, after: 100, line: 320 } } }
      ]
    },
    numbering: {
      config: [
        { reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 420, hanging: 260 } } } }] },
        { reference: 'numbers', levels: [{ level: 0, format: LevelFormat.DECIMAL, text: '%1.', alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 420, hanging: 300 } } } }] }
      ]
    },
    sections: [
      {
        properties: { page: { size: { width: PAGE.width, height: PAGE.height }, margin: { top: 1440, bottom: 1440, left: PAGE.margin, right: PAGE.margin, header: 720, footer: 720 } }, titlePage: true },
        headers: {
          default: new Header({
            children: [new Paragraph({
              alignment: AlignmentType.RIGHT,
              border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: COLOR.border, space: 4 } },
              children: [new TextRun({ text: `数据同步平台 ${title}  v${version}`, size: 16, color: COLOR.muted })]
            })]
          }),
          first: new Header({ children: [new Paragraph({ children: [] })] })
        },
        footers: {
          default: new Footer({
            children: [new Paragraph({
              alignment: AlignmentType.CENTER,
              children: [
                new TextRun({ text: '第 ', size: 16, color: COLOR.muted }),
                new TextRun({ children: [PageNumber.CURRENT], size: 16, color: COLOR.muted }),
                new TextRun({ text: ' 页 / 共 ', size: 16, color: COLOR.muted }),
                new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 16, color: COLOR.muted }),
                new TextRun({ text: ' 页', size: 16, color: COLOR.muted })
              ]
            })]
          }),
          first: new Footer({ children: [new Paragraph({ children: [] })] })
        },
        children: [...cover(title, doc.subtitle), ...body]
      }
    ]
  });
  return Packer.toBuffer(document).then(buffer => {
    fs.mkdirSync(outDir, { recursive: true });
    const file = path.join(outDir, doc.file);
    fs.writeFileSync(file, buffer);
    console.log(`${file}: ${blocks.length} blocks, ${state.figures} figures, ${Math.round(buffer.length / 1024)} KB`);
  });
}

(async () => {
  for (const doc of DOCS) await build(doc);
})().catch(err => {
  console.error(`build-docx: ${err.message}`);
  process.exit(1);
});
