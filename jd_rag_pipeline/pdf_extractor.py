"""
PDF text extraction — reads text content from downloaded PDF files.
Uses PyMuPDF (fitz) for reliable text extraction.
"""

from pathlib import Path
import fitz  # PyMuPDF


def extract_text_from_pdf(pdf_path: Path) -> str:
    """
    Extract all text from a PDF file.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        Extracted text as a single string (pages separated by newlines).
    """
    try:
        doc = fitz.open(str(pdf_path))
        pages_text = []
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text("text")
            if text.strip():
                pages_text.append(text.strip())
        doc.close()

        full_text = "\n\n".join(pages_text)

        if not full_text.strip():
            print(
                f"   ⚠️  No text extracted from '{pdf_path.name}' (might be scanned/image PDF)."
            )
            return ""

        return full_text

    except Exception as e:
        print(f"   ❌ Error extracting text from '{pdf_path.name}': {e}")
        return ""


def extract_texts_from_pdfs(pdf_paths: list[Path]) -> list[dict]:
    """
    Extract text from multiple PDFs and return structured results.

    Args:
        pdf_paths: List of PDF file paths.

    Returns:
        List of dicts with keys: filename, filepath, text, page_count, char_count
    """
    results = []

    for path in pdf_paths:
        print(f"   📖 Extracting text from: {path.name}")
        text = extract_text_from_pdf(path)

        try:
            doc = fitz.open(str(path))
            page_count = len(doc)
            doc.close()
        except Exception:
            page_count = 0

        results.append(
            {
                "filename": path.name,
                "filepath": str(path),
                "text": text,
                "page_count": page_count,
                "char_count": len(text),
            }
        )

    successful = sum(1 for r in results if r["text"])
    print(f"\n✅ Extracted text from {successful}/{len(pdf_paths)} PDF(s).")
    return results
