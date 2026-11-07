// Helpers for the PowerPredict TL report (docx v9)
const fs = require("fs");
const path = require("path");
const d = require("docx");
const {
  Paragraph, TextRun, ImageRun, Table, TableRow, TableCell, WidthType, AlignmentType, HeadingLevel,
  BorderStyle, ShadingType, VerticalAlign, PageBreak,
} = d;
const OMath = d.Math;

const FONT = "Times New Roman";
const CONTENT_DXA = 12240 - 1800 - 1440; // US Letter minus 1.25 in left / 1 in right = 9000 DXA
const CONTENT_PX = Math.floor(CONTENT_DXA / 1440 * 96); // 529 px
const IMG = path.join(__dirname, "img");

const state = { chap: 0, tab: 0, fig: 0, list: 0 };

// --- inline markup: **bold**, *italic*, `code`
function runs(text, base = {}) {
  if (Array.isArray(text)) return text;
  const out = [];
  const re = /(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    // markup wins over the base style (a table cell passes bold: undefined)
    if (t.startsWith("**")) out.push(new TextRun({ ...base, text: t.slice(2, -2), bold: true }));
    else if (t.startsWith("`")) out.push(new TextRun({ ...base, text: t.slice(1, -1), font: "Consolas", size: Math.min(base.size || 20, 20) }));
    else out.push(new TextRun({ ...base, text: t.slice(1, -1), italics: true }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...base }));
  return out;
}

const p = (text, o = {}) => new Paragraph({
  children: runs(text), alignment: o.align || AlignmentType.JUSTIFIED,
  indent: o.noIndent ? undefined : { firstLine: 567 },
  spacing: { line: 360, after: o.after ?? 120, before: o.before ?? 0 }, keepNext: o.keepNext,
});
const pn = (text, o = {}) => p(text, { ...o, noIndent: true });
const center = (text, o = {}) => new Paragraph({
  children: runs(text, o.run || {}), alignment: AlignmentType.CENTER, spacing: { line: o.line || 360, after: o.after ?? 120, before: o.before ?? 0 },
});
const blank = (n = 1) => Array.from({ length: n }, () => new Paragraph({ children: [new TextRun("")] }));
const pageBreak = () => new Paragraph({ children: [new PageBreak()] });

function h1(kap, title) {
  state.chap += 1; state.tab = 0; state.fig = 0;
  return new Paragraph({
    heading: HeadingLevel.HEADING_1, alignment: AlignmentType.CENTER, pageBreakBefore: true,
    spacing: { after: 360 },
    children: [new TextRun({ text: kap }), new TextRun({ text: title, break: 1 })],
  });
}
const h1plain = (title) => new Paragraph({ heading: HeadingLevel.HEADING_1, alignment: AlignmentType.CENTER, pageBreakBefore: true, spacing: { after: 360 }, children: [new TextRun(title)] });
const h2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] });
const h3 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(t)] });

function bullets(items, level = 0) {
  return items.map((t) => new Paragraph({
    children: runs(t), numbering: { reference: "bullets", level }, alignment: AlignmentType.JUSTIFIED,
    spacing: { line: 360, after: 60 },
  }));
}
function numbers(items) {
  state.list += 1;
  const inst = state.list;
  return items.map((t) => new Paragraph({
    children: runs(t), numbering: { reference: "numbers", level: 0, instance: inst }, alignment: AlignmentType.JUSTIFIED,
    spacing: { line: 360, after: 60 },
  }));
}

const border = { style: BorderStyle.SINGLE, size: 4, color: "808080" };
const borders = { top: border, bottom: border, left: border, right: border };

function caption(kind, text) {
  const n = kind === "Tabela" ? ++state.tab : ++state.fig;
  return new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { before: 80, after: 240, line: 276 },
    children: [new TextRun({ text: `${kind} ${state.chap}.${n}. `, bold: true, size: 22 }), ...runs(text, { size: 22, italics: false })],
  });
}

// table(headers, rows, widthsInPercent, {caption, size, firstColBold})
function table(headers, rows, widths, o = {}) {
  const total = CONTENT_DXA;
  const w = widths.map((x) => Math.round(total * x / widths.reduce((a, b) => a + b, 0)));
  const diff = total - w.reduce((a, b) => a + b, 0); w[w.length - 1] += diff;
  const size = o.size || 20;
  const cell = (t, i, head, keepNext) => new TableCell({
    borders, width: { size: w[i], type: WidthType.DXA }, verticalAlign: VerticalAlign.CENTER,
    shading: head ? { fill: "D9E2F3", type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 50, bottom: 50, left: 90, right: 90 },
    children: [new Paragraph({
      alignment: (o.align && o.align[i]) || AlignmentType.LEFT, spacing: { line: 240, after: 0 }, keepNext,
      children: runs(String(t), { size, bold: head || (o.firstColBold && i === 0) ? true : undefined }),
    })],
  });
  // the last row keeps with the caption, so a caption never starts a page alone;
  // a short table (<= 12 rows) keeps all rows together so it is never split over two pages
  const last = rows.length - 1;
  const whole = rows.length <= 12;
  const tbl = new Table({
    width: { size: total, type: WidthType.DXA }, columnWidths: w, alignment: AlignmentType.CENTER,
    rows: [new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, i, true, whole || rows.length === 0)) }),
      ...rows.map((r, ri) => new TableRow({ cantSplit: true, children: r.map((t, i) => cell(t, i, false, (whole && ri < last) || (!!o.caption && ri === last))) }))],
  });
  const out = [tbl];
  if (o.caption) out.push(caption("Tabela", o.caption));
  return out;
}

function pngSize(file) {
  const b = fs.readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20), data: b };
}
function figure(name, cap, o = {}) {
  const file = path.join(IMG, name);
  const { w, h, data } = pngSize(file);
  const width = Math.min(o.width || CONTENT_PX, CONTENT_PX);
  let height = Math.round(width * h / w);
  const maxH = o.maxHeight || 720;
  let ww = width;
  if (height > maxH) { height = maxH; ww = Math.round(maxH * w / h); }
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120, after: 40 }, keepNext: true,
      children: [new ImageRun({ type: "png", data, transformation: { width: ww, height } })] }),
    caption("Imajen", cap),
  ];
}

// --- equations
const mr = (t) => new d.MathRun(t);
const frac = (n, den) => new d.MathFraction({ numerator: [].concat(n), denominator: [].concat(den) });
const sub = (b, s) => new d.MathSubScript({ children: [].concat(b), subScript: [].concat(s) });
const sup = (b, s) => new d.MathSuperScript({ children: [].concat(b), superScript: [].concat(s) });
const subsup = (b, s, u) => new d.MathSubSuperScript({ children: [].concat(b), subScript: [].concat(s), superScript: [].concat(u) });
const sum = (body, s, u) => new d.MathSum({ children: [].concat(body), subScript: s ? [].concat(s) : undefined, superScript: u ? [].concat(u) : undefined });
const sqrt = (body) => new d.MathRadical({ children: [].concat(body) });
function eq(children, label) {
  const kids = [new OMath({ children })];
  if (label) kids.push(new TextRun({ text: `\t\t(${label})` }));
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120, after: 160 }, children: kids });
}

const fmt = (v, dgt = 0) => Number(v).toLocaleString("en-US", { minimumFractionDigits: dgt, maximumFractionDigits: dgt });
// anomaly types are written in Tetun in the report
const TT = { "Normal": "Normal", "Voltage Sag": "Voltajen Tun", "Over-Voltage": "Voltajen Sa'e Liu",
  "Frequency Deviation": "Frekuénsia La Estável", "Overload": "Karga Liu", "Energy Theft": "Na'ok Eletrisidade",
  "Meter Fault": "Kontador Aat" };
const tt = (name) => TT[name] || name;
const TYPES = ["Normal", "Voltage Sag", "Over-Voltage", "Frequency Deviation", "Overload", "Energy Theft", "Meter Fault"];
const FEAT = { voltage_v: "Voltage (V)", voltage_dev_pct: "Voltage deviation (%)", current_a: "Current (A)", power_kw: "Power (kW)",
  power_factor: "Power factor", frequency_hz: "Frequency (Hz)", freq_dev_hz: "Frequency deviation (Hz)", temperature_c: "Temperature (°C)",
  contracted_kva: "Contracted kVA", load_ratio: "Load ratio", baseline_ratio: "Baseline ratio", consistency_ratio: "Consistency ratio",
  hour: "Hour", is_weekend: "Weekend", customer_type: "Customer type", municipality: "Municipality" };
const fl = (k) => FEAT[k] || k;
const pct = (v, dgt = 2) => (Number(v) * 100).toFixed(dgt) + "%";

module.exports = {
  d, FONT, CONTENT_DXA, CONTENT_PX, state, runs, p, pn, center, blank, pageBreak, h1, h1plain, h2, h3, bullets, numbers,
  table, figure, caption, eq, mr, frac, sub, sup, subsup, sum, sqrt, fmt, pct, AlignmentType, TT, tt, TYPES, FEAT, fl,
};
