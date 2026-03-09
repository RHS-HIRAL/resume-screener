import os
import json
from pathlib import Path
from datetime import datetime
from typing import List

from dotenv import load_dotenv
from pydub import AudioSegment  # noqa: F401  (needed by Sarvam's SDK implicitly)
from sarvamai import SarvamAI
from google import genai
from google.genai import types


# ---------------------------------------------------------------------------
# Load environment variables (.env lives three levels up from this file)
# ---------------------------------------------------------------------------
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)

SARVAM_API_KEY = os.getenv("SARVAM_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

if not SARVAM_API_KEY:
    raise EnvironmentError("SARVAM_API_KEY is not set. Add it to your .env file.")
if not GOOGLE_API_KEY:
    raise EnvironmentError("GOOGLE_API_KEY is not set. Add it to your .env file.")

# ---------------------------------------------------------------------------
# Paths (all support files sit next to this script)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROMPT_TEMPLATE_PATH = BASE_DIR / "prompt_template.txt"
QA_PATH = BASE_DIR / "QA.txt"
STT_OUTPUT_DIR = BASE_DIR / "outputs"  # Sarvam STT job outputs
EVAL_OUTPUT_DIR = BASE_DIR / "structured_output"  # Gemini eval outputs

STT_OUTPUT_DIR.mkdir(exist_ok=True)
EVAL_OUTPUT_DIR.mkdir(exist_ok=True)

GEMINI_MODEL_ID = "gemini-2.5-flash"


# ===========================================================================
# STAGE 1 — Sarvam Speech-to-Text  (voice note -> _conversation.txt)
# ===========================================================================


def transcribe_audio(audio_path: str) -> str:
    """
    Submit *audio_path* to Sarvam STT, wait for completion, write the
    diarised conversation to a *_conversation.txt* file, and return the
    path to that file.

    Only the _conversation.txt is produced; timing / analysis files from
    the original sarvam.py are intentionally omitted.
    """
    client = SarvamAI(api_subscription_key=SARVAM_API_KEY)

    print(f"\n[STT] Submitting audio file: {audio_path}")
    job = client.speech_to_text_translate_job.create_job(
        model="saaras:v3",
        with_diarization=True,
    )
    job.upload_files(file_paths=[audio_path], timeout=300)
    job.start()

    print("[STT] Waiting for transcription to complete ...")
    job.wait_until_complete()

    if job.is_failed():
        raise RuntimeError("Sarvam STT job failed.")

    # Download raw JSON outputs into a job-specific sub-folder
    output_dir = STT_OUTPUT_DIR / f"transcriptions_{job.job_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    job.download_outputs(output_dir=str(output_dir))

    json_files = list(output_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No .json transcription files found in {output_dir}.")

    # Parse the first (and usually only) JSON result
    conversation_txt_path = _parse_and_save_conversation(json_files[0], output_dir)

    print(f"[STT] Conversation saved to: {conversation_txt_path}")
    return str(conversation_txt_path)


def _parse_and_save_conversation(json_file: Path, output_dir: Path) -> Path:
    """
    Read a Sarvam STT JSON result, extract diarised lines, write them to
    <stem>_conversation.txt, and return that path.
    """
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    diarized = data.get("diarized_transcript", {}).get("entries")
    lines: List[str] = []

    if diarized:
        for entry in diarized:
            speaker = entry["speaker_id"]
            text = entry["transcript"]
            lines.append(f"{speaker}: {text}")
    else:
        # Fallback: un-diarised transcript
        lines = [f"UNKNOWN: {data.get('transcript', '')}"]

    conversation_text = "\n".join(lines)
    txt_path = output_dir / f"{json_file.stem}_conversation.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(conversation_text)

    return txt_path


# ===========================================================================
# STAGE 2 — Gemini QA Scoring  (transcript text -> evaluation report)
# ===========================================================================


def load_text(path: Path) -> str:
    """Read a text file and return its contents as a string."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_prompt(template: str, qa: str, transcript: str) -> str:
    """
    Fill the prompt template with the QA reference sheet and transcript.

    The template uses two placeholders:
        {QA}         - replaced with the contents of QA.txt
        {TRANSCRIPT} - replaced with the transcribed conversation text
    """
    return template.format(QA=qa, TRANSCRIPT=transcript)


def score_interview(prompt: str) -> tuple[str, dict]:
    """
    Send the filled prompt to Gemini and return:
        (response_text, token_metadata_dict)
    """
    client = genai.Client(api_key=GOOGLE_API_KEY)

    response = client.models.generate_content(
        model=GEMINI_MODEL_ID,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,  # low temperature for consistent scoring
        ),
    )

    text = response.text
    usage = response.usage_metadata
    token_meta = {
        "prompt_tokens": getattr(usage, "prompt_token_count", "N/A"),
        "candidates_tokens": getattr(usage, "candidates_token_count", "N/A"),
        "total_tokens": getattr(usage, "total_token_count", "N/A"),
        "model": GEMINI_MODEL_ID,
    }
    return text, token_meta


def format_token_block(meta: dict) -> str:
    """Return a human-readable token usage summary string."""
    return (
        "\n"
        "+-----------------------------------------+\n"
        "|          TOKEN USAGE METADATA            |\n"
        "+-----------------------------------------+\n"
        f"|  Model              : {meta['model']:<20}|\n"
        f"|  Prompt tokens      : {str(meta['prompt_tokens']):<20}|\n"
        f"|  Candidate tokens   : {str(meta['candidates_tokens']):<20}|\n"
        f"|  Total tokens       : {str(meta['total_tokens']):<20}|\n"
        "+-----------------------------------------+\n"
    )


def save_results(result_text: str, token_meta: dict, timestamp: str) -> Path:
    """
    Save the evaluation result and token metadata to a timestamped file.
    Returns the path of the saved file.
    """
    output_path = EVAL_OUTPUT_DIR / f"eval_results_{timestamp}.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"Evaluation Run : {timestamp}\n")
        f.write("=" * 70 + "\n")
        f.write("GEMINI EVALUATION RESULT:\n")
        f.write("=" * 70 + "\n")
        f.write(result_text)
        f.write("\n")
        f.write(format_token_block(token_meta))
    return output_path


# ===========================================================================
# PIPELINE — wire Stage 1 and Stage 2 together
# ===========================================================================


def run_pipeline(audio_path: str) -> None:
    """
    Full end-to-end pipeline:
      1. Transcribe *audio_path* via Sarvam STT  ->  saves _conversation.txt
      2. Load that transcript and score it with Gemini QA scoring
      3. Save evaluation results to structured_output/
    """
    # ------------------------------------------------------------------
    # Stage 1: Speech -> Text
    # ------------------------------------------------------------------
    conversation_txt_path = transcribe_audio(audio_path)

    # ------------------------------------------------------------------
    # Stage 2: QA Scoring
    # ------------------------------------------------------------------
    template_raw = load_text(PROMPT_TEMPLATE_PATH)
    qa_text = load_text(QA_PATH)
    transcript = load_text(Path(conversation_txt_path))

    # The template file may wrap its string in a Python assignment:
    #   prompt_template = """..."""
    # Extract only the content between the triple-quotes if so.
    if "prompt_template" in template_raw and '"""' in template_raw:
        start = template_raw.index('"""') + 3
        end = template_raw.rindex('"""')
        template_str = template_raw[start:end]
    else:
        template_str = template_raw

    prompt = build_prompt(template_str, qa=qa_text, transcript=transcript)

    print("\n" + "=" * 70)
    print("PROMPT SENT TO GEMINI:")
    print("=" * 70)
    print(prompt)
    print("=" * 70)

    print(f"\n[QA] Calling Gemini API ({GEMINI_MODEL_ID}) ...\n")
    result, token_meta = score_interview(prompt)

    print("=" * 70)
    print("GEMINI EVALUATION RESULT:")
    print("=" * 70)
    print(result)
    print(format_token_block(token_meta))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = save_results(result, token_meta, timestamp)
    print(f"\n[QA] Results saved to: {output_path}")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    # Update this path to point to your audio file
    AUDIO_FILE = (
        "C:/Hiral/Work/Si2 Resume Screener/resumer screener setup/"
        "call_transcribe/audio_files/1.mp3"
    )
    run_pipeline(AUDIO_FILE)
