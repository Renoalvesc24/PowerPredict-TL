// Build the PowerPredict TL capstone report (Tetun) -> ../PowerPredict_TL_Relatoriu_Capstone_BigData.docx
const fs = require("fs");
const path = require("path");
const L = require("./lib");
const d = L.d;
const { Document, Packer, Paragraph, TextRun, ImageRun, AlignmentType, Footer, PageNumber, LevelFormat, HeadingLevel } = d;

const F = JSON.parse(fs.readFileSync(path.join(__dirname, "facts.json"), "utf8"));
const FONT = L.FONT;

// ----------------------------------------------------------------- cover (layout of the original report)
function cover() {
  const logo = fs.readFileSync(path.join(__dirname, "img", "cover_logo_0.png"));
  const c = (text, o = {}) => new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { before: o.before || 0, after: o.after ?? 160, line: o.line || 276 },
    children: [new TextRun({ text, bold: o.bold ?? true, size: o.size || 24, font: FONT })],
  });
  return [
    ...L.blank(1),
    c("RELATÓRIU FINÁL CAPSTONE PROJECT", { after: 480 }),
    c("Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)", { size: 28, after: 480, line: 320 }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 480 },
      children: [new ImageRun({ type: "png", data: logo, transformation: { width: 300, height: 214 } })] }),
    c("Prepara husi (Grupu 7):", { after: 160 }),
    c("Reinaldo N. M. Alves Costa [2301020110084]", { after: 120 }),
    c("Alexandre E. Lisboa Ximenes [2301020110003]", { after: 160 }),
    c("UNIDADE KURIKULER: BIG DATA", { after: 960 }),
    c("DEPARTAMENTU SIENSIA KOMPUTADOR", { after: 200 }),
    c("FAKULDADE SIENSIA NO ENJENHARIA", { after: 200 }),
    c("DILI INSTITUTE OF TECHNOLOGY", { after: 200 }),
    c("2026", { after: 0 }),
  ];
}

const body = [
  ...cover(),
  ...require("./ch1_2")(F),
  ...require("./ch3")(F),
  ...require("./ch4_design")(F),
  ...require("./ch5_results")(F),
  ...require("./ch6_refs")(F),
];

const doc = new Document({
  creator: "Reinaldo N. M. Alves Costa; Alexandre E. Lisboa Ximenes",
  title: "Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)",
  description: "Deteksaun anomalia rede eletrisidade real-time ho Big Data no Random Forest",
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: FONT, size: 24 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 28, bold: true }, paragraph: { spacing: { before: 0, after: 360 }, alignment: AlignmentType.CENTER, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 24, bold: true }, paragraph: { spacing: { before: 240, after: 120 }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 24, bold: true }, paragraph: { spacing: { before: 180, after: 100 }, keepNext: true, outlineLevel: 2 } },
    ],
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [
        { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
        { level: 1, format: LevelFormat.BULLET, text: "◦", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1080, hanging: 360 } } } }] },
      { reference: "numbers", levels: [
        { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections: [{
    properties: {
      titlePage: true,
      page: { size: { width: 12240, height: 15840 }, margin: { top: 1440, bottom: 1440, left: 1800, right: 1440, footer: 600 } },
    },
    footers: {
      default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ children: [PageNumber.CURRENT], size: 22 })] })] }),
      first: new Footer({ children: [new Paragraph({ children: [] })] }),
    },
    children: body,
  }],
});

const outFile = path.resolve(process.argv[2] || path.join(__dirname, "..", "PowerPredict_TL_Relatoriu_Capstone_BigData.docx"));
Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync(outFile, buf);
  console.log("written", outFile, (buf.length / 1024 / 1024).toFixed(2), "MB");
});
