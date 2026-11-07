# Open the report in Microsoft Word, refresh the table of contents, save, export PDF.
# Safe when the user already has Word open: if COM attaches to that Word (other documents open),
# the script does not hide it or quit it - it only closes the document it opened.
param([string]$Docx)
$ErrorActionPreference = "Stop"
$Docx = (Resolve-Path $Docx).Path
$pdf = [System.IO.Path]::ChangeExtension($Docx, ".pdf")
$word = New-Object -ComObject Word.Application
$shared = $word.Documents.Count -gt 0
if (-not $shared) { $word.Visible = $false }
$oldAlerts = $word.DisplayAlerts
$word.DisplayAlerts = 0
$doc = $null
try {
    $doc = $word.Documents.Open($Docx, $false, $false, $false)
    foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
    $doc.Fields.Update() | Out-Null
    foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
    $doc.Save()
    # 17 = wdExportFormatPDF; last argument 1 = wdExportCreateHeadingBookmarks (PDF bookmarks from the headings)
    $doc.ExportAsFixedFormat($pdf, 17, $false, 0, 0, 1, 1, 0, $true, $true, 1)
    "pages: " + $doc.ComputeStatistics(2)
    "pdf: $pdf"
} finally {
    if ($doc) { $doc.Close($false) }
    $word.DisplayAlerts = $oldAlerts
    if (-not $shared) { $word.Quit() } else { "Word was already open - left it running" }
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($word) | Out-Null
}
