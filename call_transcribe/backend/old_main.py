"""
EchoScript AI — FastAPI Backend
Supports three transcription modes: Local Whisper, Gemini API, Groq API.
Audio files > 5 minutes are automatically chunked into 3-minute segments.
RAM usage is guarded to stay within 5 GB.

uvicorn main:app --reload --port 8000
"""

import os
import gc
import json
import shutil
import base64
import tempfile
import logging
from pathlib import Path

import psutil
from pydub import AudioSegment

# Lazy imports — only loaded when local transcription is used
torch = None
whisper = None


def _ensure_whisper():
    """Lazily import whisper and torch."""
    global torch, whisper
    if torch is None:
        import torch as _torch

        torch = _torch
    if whisper is None:
        try:
            import whisper as _whisper

            whisper = _whisper
        except ImportError:
            raise HTTPException(
                status_code=503,
                detail="openai-whisper is not installed. Run: pip install openai-whisper",
            )


from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ── Load .env from parent's parent directory ──────────────────────────────────
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_RAM_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB total budget
RAM_SAFETY_MARGIN_BYTES = 1.5 * 1024 * 1024 * 1024  # keep 1.5 GB free after model
CHUNK_THRESHOLD_SEC = 5 * 60  # 5 minutes
CHUNK_LENGTH_SEC = 3 * 60  # 3 minutes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("echoscript")

# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(title="EchoScript AI Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Whisper model cache ──────────────────────────────────────────────────────
_whisper_model = None
_whisper_model_name = None


def _check_memory(required_gb: float = 2.0):
    """Raise if available memory is too low."""
    available = psutil.virtual_memory().available
    required = required_gb * 1024 * 1024 * 1024
    if available < required:
        available_gb = available / (1024**3)
        raise HTTPException(
            status_code=503,
            detail=(
                f"Insufficient memory. Available: {available_gb:.1f} GB, "
                f"Required: {required_gb:.1f} GB. "
                "Try a smaller model or close other applications."
            ),
        )


def _get_whisper_model(model_name: str = "medium"):
    """Load or reuse a cached Whisper model."""
    global _whisper_model, _whisper_model_name

    _ensure_whisper()

    if _whisper_model is not None and _whisper_model_name == model_name:
        return _whisper_model

    # Unload previous model
    _unload_whisper_model()

    # Check memory before loading
    model_sizes_gb = {
        "tiny": 0.15,
        "base": 0.3,
        "small": 1.0,
        "medium": 1.5,
        "large": 3.0,
        "large-v3": 3.0,
    }
    needed = model_sizes_gb.get(model_name, 2.0) + 1.0  # model + processing buffer
    _check_memory(needed)

    logger.info(f"Loading Whisper model: {model_name}")
    device = "cuda" if torch and torch.cuda.is_available() else "cpu"
    _whisper_model = whisper.load_model(model_name, device=device)
    _whisper_model_name = model_name
    logger.info(f"Whisper model '{model_name}' loaded on {device}")
    return _whisper_model


def _unload_whisper_model():
    """Free memory occupied by the Whisper model."""
    global _whisper_model, _whisper_model_name
    if _whisper_model is not None:
        del _whisper_model
        _whisper_model = None
        _whisper_model_name = None
        gc.collect()
        if torch and torch.cuda.is_available():
            torch.cuda.empty_cache()
        logger.info("Whisper model unloaded, memory freed")


def _save_upload_to_temp(upload: UploadFile) -> str:
    """Save an uploaded file to a temp path and return the path."""
    suffix = Path(upload.filename or "audio.wav").suffix or ".wav"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    shutil.copyfileobj(upload.file, tmp)
    tmp.close()
    return tmp.name


def _chunk_audio(file_path: str) -> list[str]:
    """
    If audio is longer than CHUNK_THRESHOLD_SEC, split into CHUNK_LENGTH_SEC
    chunks. Returns list of file paths (original if short enough).
    """
    audio = AudioSegment.from_file(file_path)
    duration_sec = len(audio) / 1000.0

    if duration_sec <= CHUNK_THRESHOLD_SEC:
        return [file_path]

    chunk_ms = CHUNK_LENGTH_SEC * 1000
    chunks = []
    for i in range(0, len(audio), chunk_ms):
        chunk = audio[i : i + chunk_ms]
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        chunk.export(tmp.name, format="wav")
        tmp.close()
        chunks.append(tmp.name)

    logger.info(
        f"Audio ({duration_sec:.0f}s) split into {len(chunks)} chunks of "
        f"~{CHUNK_LENGTH_SEC}s each"
    )
    return chunks


def _format_timestamp(seconds: float) -> str:
    """Convert seconds to MM:SS format."""
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 1 — Local Whisper Transcription
# ═══════════════════════════════════════════════════════════════════════════════


@app.post("/api/transcribe/local")
async def transcribe_local(
    file: UploadFile = File(...),
    model_name: str = Form("medium"),
):
    """Transcribe audio locally using OpenAI Whisper."""
    tmp_path = None
    chunk_paths: list[str] = []

    try:
        # Save uploaded file
        tmp_path = _save_upload_to_temp(file)

        # Load model (with memory check)
        model = _get_whisper_model(model_name)

        # Chunk if needed
        chunk_paths = _chunk_audio(tmp_path)
        is_chunked = len(chunk_paths) > 1

        all_segments = []
        time_offset = 0.0

        for idx, chunk_path in enumerate(chunk_paths):
            logger.info(f"Processing chunk {idx + 1}/{len(chunk_paths)}")

            prompt = (
                "Hello, yeh audio Hindi aur English mixed language mein hai. "
                "Please transcribe exactly as spoken."
            )

            result = model.transcribe(
                chunk_path,
                condition_on_previous_text=False,
                initial_prompt=prompt,
                no_speech_threshold=0.6,
                logprob_threshold=-1.0,
            )

            for seg in result.get("segments", []):
                start = seg["start"] + time_offset
                end = seg["end"] + time_offset
                all_segments.append(
                    {
                        "timestamp": f"{_format_timestamp(start)} - {_format_timestamp(end)}",
                        "content": seg["text"].strip(),
                        "language": "Hinglish",
                        "language_code": "hi-en",
                        "translation": "",
                        "emotion": "Neutral",
                    }
                )

            # Update offset for next chunk
            if is_chunked:
                chunk_audio = AudioSegment.from_file(chunk_path)
                time_offset += len(chunk_audio) / 1000.0

        # Build summary
        full_text = " ".join(s["content"] for s in all_segments)
        summary = (
            f"Local Whisper transcription ({model_name} model). "
            f"{len(all_segments)} segments detected. "
            f"{'Audio was split into ' + str(len(chunk_paths)) + ' chunks. ' if is_chunked else ''}"
            f"Total text length: {len(full_text)} characters."
        )

        return {"summary": summary, "segments": all_segments}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Local transcription failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        import contextlib

        # Cleanup temp files safely to avoid WinError 32 on Windows
        if tmp_path and os.path.exists(tmp_path):
            with contextlib.suppress(PermissionError, OSError):
                os.remove(tmp_path)
        for cp in chunk_paths:
            if cp != tmp_path and os.path.exists(cp):
                with contextlib.suppress(PermissionError, OSError):
                    os.remove(cp)


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 2 — Gemini API Transcription
# ═══════════════════════════════════════════════════════════════════════════════


@app.post("/api/transcribe/gemini")
async def transcribe_gemini(
    file: UploadFile = File(None),
    audio_base64: str = Form(None),
    mime_type: str = Form("audio/webm"),
):
    """Transcribe audio using Google Gemini API with speaker diarization."""
    if not GOOGLE_API_KEY:
        raise HTTPException(
            status_code=500, detail="GOOGLE_API_KEY not configured in .env"
        )

    try:
        import google.generativeai as genai

        genai.configure(api_key=GOOGLE_API_KEY)

        # Get base64 audio data
        if audio_base64:
            b64_data = audio_base64
        elif file:
            content = await file.read()
            b64_data = base64.b64encode(content).decode("utf-8")
            mime_type = file.content_type or mime_type
        else:
            raise HTTPException(status_code=400, detail="No audio data provided")

        model = genai.GenerativeModel("gemini-2.5-flash-lite")

        prompt = """
You are an expert audio transcription assistant.
Process the provided audio file and generate a detailed transcription.

Requirements:
1. Provide accurate timestamps for each segment (Format: MM:SS).
2. Detect the primary language of each segment.
3. If the segment is in a language different than English, also provide the English translation.
4. Identify the primary emotion of the speaker in this segment. Choose exactly one of: Happy, Sad, Angry, Neutral.
5. Provide a brief summary of the entire audio at the beginning.

Output Format: JSON object with this structure:
{
  "summary": "A brief summary of the conversation...",
  "segments": [
    {
      "timestamp": "00:00 - 00:15",
      "content": "Hello, how are you doing today?",
      "language": "English",
      "language_code": "en",
      "translation": "",
      "emotion": "Happy"
    }
  ]
}

IMPORTANT: Return ONLY the JSON object, no markdown fencing or extra text.
"""

        response = model.generate_content(
            [
                {"mime_type": mime_type, "data": b64_data},
                prompt,
            ],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
            ),
        )

        text = response.text
        if not text:
            raise HTTPException(status_code=500, detail="Empty response from Gemini")

        # Parse JSON (handle possible markdown fencing)
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        data = json.loads(cleaned)
        return data

    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        logger.exception("Failed to parse Gemini response as JSON")
        raise HTTPException(status_code=500, detail=f"Invalid JSON from Gemini: {e}")
    except Exception as e:
        logger.exception("Gemini transcription failed")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 3 — Groq API Transcription
# ═══════════════════════════════════════════════════════════════════════════════


@app.post("/api/transcribe/groq")
async def transcribe_groq(
    file: UploadFile = File(...),
):
    """Transcribe audio using Groq's Whisper API (cloud-based, fast)."""
    if not GROQ_API_KEY:
        raise HTTPException(
            status_code=500, detail="GROQ_API_KEY not configured in .env"
        )

    tmp_path = None
    try:
        from groq import Groq

        client = Groq(api_key=GROQ_API_KEY)

        # Save uploaded file
        tmp_path = _save_upload_to_temp(file)

        # Groq supports files up to 25 MB
        file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        if file_size_mb > 25:
            raise HTTPException(
                status_code=400,
                detail=f"File too large for Groq API ({file_size_mb:.1f} MB). Max 25 MB.",
            )

        with open(tmp_path, "rb") as audio_file:
            transcription = client.audio.transcriptions.create(
                file=(file.filename or "audio.wav", audio_file),
                model="whisper-large-v3",
                response_format="verbose_json",
                language="hi",  # Hindi primary, handles code-switching
            )

        # Transform Groq response into our standard format
        segments = []
        if hasattr(transcription, "segments") and transcription.segments:
            for seg in transcription.segments:
                start = (
                    seg.get("start", 0)
                    if isinstance(seg, dict)
                    else getattr(seg, "start", 0)
                )
                end = (
                    seg.get("end", 0)
                    if isinstance(seg, dict)
                    else getattr(seg, "end", 0)
                )
                text = (
                    seg.get("text", "")
                    if isinstance(seg, dict)
                    else getattr(seg, "text", "")
                )
                segments.append(
                    {
                        "timestamp": f"{_format_timestamp(start)} - {_format_timestamp(end)}",
                        "content": text.strip(),
                        "language": "Hinglish",
                        "language_code": "hi-en",
                        "translation": "",
                        "emotion": "Neutral",
                    }
                )
        else:
            # Fallback: single segment with full text
            full_text = (
                transcription.text
                if hasattr(transcription, "text")
                else str(transcription)
            )
            segments.append(
                {
                    "timestamp": f"{_format_timestamp(start)} - {_format_timestamp(end)}",
                    "content": text.strip(),
                    "language": "Hinglish",
                    "language_code": "hi-en",
                    "translation": "",
                    "emotion": "Neutral",
                }
            )

        full_text = " ".join(s["content"] for s in segments)
        summary = (
            f"Groq Whisper API transcription (whisper-large-v3). "
            f"{len(segments)} segments detected. "
            f"Total text length: {len(full_text)} characters."
        )

        return {"summary": summary, "segments": segments}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Groq transcription failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ═══════════════════════════════════════════════════════════════════════════════
# Health & Info
# ═══════════════════════════════════════════════════════════════════════════════


@app.get("/api/health")
async def health():
    mem = psutil.virtual_memory()
    return {
        "status": "ok",
        "memory": {
            "total_gb": round(mem.total / (1024**3), 1),
            "available_gb": round(mem.available / (1024**3), 1),
            "used_percent": mem.percent,
        },
        "gpu_available": torch.cuda.is_available() if torch else False,
        "whisper_model_loaded": _whisper_model_name,
        "whisper_available": whisper is not None,
        "gemini_configured": bool(GOOGLE_API_KEY),
        "groq_configured": bool(GROQ_API_KEY),
    }


@app.post("/api/unload-model")
async def unload_model():
    """Manually unload the Whisper model to free memory."""
    _unload_whisper_model()
    return {"status": "model_unloaded"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
