import os
import json
import shutil
import base64
import tempfile
import logging
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("echoscript")

app = FastAPI(title="EchoScript AI Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _save_upload_to_temp(upload: UploadFile) -> str:
    suffix = Path(upload.filename or "audio.wav").suffix or ".wav"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    shutil.copyfileobj(upload.file, tmp)
    tmp.close()
    return tmp.name


def _format_timestamp(seconds: float) -> str:
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"


@app.post("/api/transcribe/gemini")
async def transcribe_gemini(
    file: UploadFile = File(None),
    audio_base64: str = Form(None),
    mime_type: str = Form("audio/webm"),
):
    if not GOOGLE_API_KEY:
        raise HTTPException(
            status_code=500, detail="GOOGLE_API_KEY not configured in .env"
        )

    try:
        import google.generativeai as genai

        genai.configure(api_key=GOOGLE_API_KEY)

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
            [{"mime_type": mime_type, "data": b64_data}, prompt],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json"
            ),
        )

        text = response.text
        if not text:
            raise HTTPException(status_code=500, detail="Empty response from Gemini")

        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        data = json.loads(cleaned)
        return data

    except Exception as e:
        logger.exception("Gemini transcription failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/transcribe/groq")
async def transcribe_groq(file: UploadFile = File(...)):
    if not GROQ_API_KEY:
        raise HTTPException(
            status_code=500, detail="GROQ_API_KEY not configured in .env"
        )

    tmp_path = None
    try:
        from groq import Groq

        client = Groq(api_key=GROQ_API_KEY)

        tmp_path = _save_upload_to_temp(file)
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
                language="hi",
            )

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
            full_text = (
                transcription.text
                if hasattr(transcription, "text")
                else str(transcription)
            )
            segments.append(
                {
                    "timestamp": "00:00 - 00:00",
                    "content": full_text.strip(),
                    "language": "Hinglish",
                    "language_code": "hi-en",
                    "translation": "",
                    "emotion": "Neutral",
                }
            )

        full_text = " ".join(s["content"] for s in segments)
        summary = f"Groq Whisper API transcription. {len(segments)} segments detected. Total text length: {len(full_text)} characters."

        return {"summary": summary, "segments": segments}

    except Exception as e:
        logger.exception("Groq transcription failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        import contextlib

        if tmp_path and os.path.exists(tmp_path):
            with contextlib.suppress(PermissionError, OSError):
                os.remove(tmp_path)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "gemini_configured": bool(GOOGLE_API_KEY),
        "groq_configured": bool(GROQ_API_KEY),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
