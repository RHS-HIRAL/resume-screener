"""
JD Metadata Parser — extracts structured metadata fields from raw JD text
using pattern matching and heuristics.

Extracted fields:
    - job_title, location, job_type, department, shifts, experience,
      job_category, positions, designation, work_hours, compensation,
      employment_type, skills, qualifications, responsibilities (summary)
"""

import re
from dataclasses import dataclass, field, asdict


@dataclass
class JDMetadata:
    """Structured metadata extracted from a Job Description."""

    filename: str = ""
    job_title: str = ""
    location: str = ""
    job_type: str = ""
    department: str = ""
    shifts: str = ""
    experience: str = ""
    job_category: str = ""
    positions: str = ""
    designation: str = ""
    work_hours: str = ""
    compensation: str = ""
    employment_type: str = ""
    skills: str = ""
    qualifications: str = ""
    responsibilities_summary: str = ""
    raw_text: str = ""

    def to_pinecone_metadata(self) -> dict:
        """
        Convert to a flat dict suitable for Pinecone metadata.
        Pinecone metadata values must be str, int, float, bool, or list[str].
        Excludes raw_text to keep metadata compact.
        """
        data = asdict(self)
        data.pop("raw_text", None)
        # Remove empty fields to save space
        return {k: v for k, v in data.items() if v}


# ---------------------------------------------------------------------------
# Pattern-based extraction helpers
# ---------------------------------------------------------------------------


def _search(pattern: str, text: str, flags=re.IGNORECASE) -> str:
    """Return first capture group or empty string."""
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else ""


def _extract_section(header_pattern: str, text: str, max_chars: int = 500) -> str:
    """
    Extract text under a section header until the next header or max_chars.
    """
    match = re.search(header_pattern, text, re.IGNORECASE | re.MULTILINE)
    if not match:
        return ""
    start = match.end()
    # Find next section header (line starting with a capitalized word followed by colon or all caps line)
    next_header = re.search(r"\n\s*(?:[A-Z][A-Za-z\s]+:|\n[A-Z]{3,})", text[start:])
    end = start + next_header.start() if next_header else start + max_chars
    return text[start:end].strip()[:max_chars]


def parse_jd_metadata(text: str, filename: str = "") -> JDMetadata:
    """
    Parse a JD's raw text into structured metadata using regex/heuristics.

    This works well for reasonably formatted JDs. For highly unstructured
    documents, consider using an LLM-based parser (see parse_jd_with_llm).
    """
    meta = JDMetadata(filename=filename, raw_text=text)

    if not text.strip():
        return meta

    # --- Job Title ---
    # Often the first prominent line, or labelled "Job Title:", "Position:", "Role:"
    meta.job_title = (
        _search(r"(?:job\s*title|position|role|designation)\s*[:\-–]\s*(.+)", text)
        or _search(r"^(.+?)(?:\n|$)", text.strip())  # fallback: first line
    )

    # --- Location ---
    meta.location = _search(
        r"(?:location|city|work\s*location|office|based\s*(?:in|at))\s*[:\-–]\s*(.+)",
        text,
    )

    # --- Job Type / Employment Type ---
    meta.job_type = _search(
        r"(?:job\s*type|type\s*of\s*(?:job|role))\s*[:\-–]\s*(.+)", text
    )
    meta.employment_type = _search(
        r"(?:employment\s*type|contract\s*type|engagement\s*type)\s*[:\-–]\s*(.+)", text
    )
    if not meta.employment_type:
        # Infer from text
        lower = text.lower()
        for etype in [
            "full-time",
            "full time",
            "part-time",
            "part time",
            "contract",
            "freelance",
            "intern",
            "temporary",
        ]:
            if etype in lower:
                meta.employment_type = etype.replace(" ", "-").title()
                break

    # --- Department ---
    meta.department = _search(
        r"(?:department|team|division|business\s*unit|function)\s*[:\-–]\s*(.+)", text
    )

    # --- Shifts ---
    meta.shifts = _search(r"(?:shift|shifts|working\s*shift)\s*[:\-–]\s*(.+)", text)

    # --- Experience ---
    meta.experience = _search(
        r"(?:experience|years?\s*of\s*experience|exp)\s*[:\-–]\s*(.+)", text
    )
    if not meta.experience:
        # Try patterns like "3-5 years", "minimum 2 years"
        meta.experience = _search(r"(\d+\s*[\-–to]+\s*\d+\s*(?:years?|yrs?))", text)
        if not meta.experience:
            meta.experience = _search(
                r"(?:minimum|min|at\s*least)\s*(\d+\s*(?:years?|yrs?))", text
            )

    # --- Job Category ---
    meta.job_category = _search(
        r"(?:job\s*category|category|domain|field)\s*[:\-–]\s*(.+)", text
    )

    # --- Positions / Openings ---
    meta.positions = _search(
        r"(?:no\.?\s*of\s*positions?|openings?|vacancies?|positions?\s*available)\s*[:\-–]\s*(.+)",
        text,
    )

    # --- Designation ---
    meta.designation = _search(
        r"(?:designation|grade|level|band)\s*[:\-–]\s*(.+)", text
    )

    # --- Work Hours ---
    meta.work_hours = _search(
        r"(?:work\s*hours?|working\s*hours?|hours?\s*per\s*week|schedule)\s*[:\-–]\s*(.+)",
        text,
    )

    # --- Compensation ---
    meta.compensation = _search(
        r"(?:salary|compensation|pay|ctc|package|remuneration)\s*[:\-–]\s*(.+)", text
    )
    if not meta.compensation:
        # Try currency patterns like "$80,000", "₹10 LPA"
        meta.compensation = _search(
            r"([\$₹€£]\s*[\d,\.]+\s*(?:[-–]\s*[\$₹€£]?\s*[\d,\.]+)?(?:\s*(?:per|/)\s*(?:year|month|annum|hr|hour|LPA|CTC))?)",
            text,
        )

    # --- Skills ---
    skills_section = _extract_section(
        r"(?:required\s*skills?|key\s*skills?|technical\s*skills?|skills?\s*required|must\s*have)\s*[:\-–]?",
        text,
        max_chars=600,
    )
    meta.skills = skills_section if skills_section else ""

    # --- Qualifications ---
    qual_section = _extract_section(
        r"(?:qualifications?|education|eligibility|requirements?|who\s*should\s*apply)\s*[:\-–]?",
        text,
        max_chars=400,
    )
    meta.qualifications = qual_section if qual_section else ""

    # --- Responsibilities (summary) ---
    resp_section = _extract_section(
        r"(?:responsibilities|key\s*responsibilities|duties|what\s*you.?ll\s*do|role\s*description)\s*[:\-–]?",
        text,
        max_chars=500,
    )
    meta.responsibilities_summary = resp_section if resp_section else ""

    return meta


def parse_all_jds(extracted_docs: list[dict]) -> list[JDMetadata]:
    """
    Parse metadata from a list of extracted document dicts.

    Args:
        extracted_docs: Output from pdf_extractor.extract_texts_from_pdfs()

    Returns:
        List of JDMetadata objects.
    """
    results = []
    for doc in extracted_docs:
        if not doc["text"]:
            print(f"   ⏭️  Skipping '{doc['filename']}' (no text).")
            continue
        print(f"   🔍 Parsing metadata from: {doc['filename']}")
        meta = parse_jd_metadata(doc["text"], filename=doc["filename"])
        results.append(meta)

    print(f"\n✅ Parsed metadata for {len(results)} JD(s).")
    return results
