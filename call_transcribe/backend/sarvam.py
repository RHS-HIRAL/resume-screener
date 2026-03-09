import os
import json
from dotenv import load_dotenv
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from pydub import AudioSegment
from sarvamai import SarvamAI
import textwrap

OUTPUT_DIR = "outputs"
Path(OUTPUT_DIR).mkdir(exist_ok=True)


def split_audio(
    audio_path: str, chunk_duration_ms: int = 60 * 60 * 1000
) -> List[AudioSegment]:
    audio = AudioSegment.from_file(audio_path)
    return (
        [
            audio[i : i + chunk_duration_ms]
            for i in range(0, len(audio), chunk_duration_ms)
        ]
        if len(audio) > chunk_duration_ms
        else [audio]
    )


ANALYSIS_PROMPT_TEMPLATE = """
Analyze this call transcription thoroughly from start to finish.

TRANSCRIPTION:
{transcription}

Please answer the following:

1. Identify which speaker is the **customer** and which one is the **agent**.
2. Determine if the customer is a **new/potential customer** or an **existing customer**.
3. What **problem, query, or doubt** did the customer raise at the beginning?
4. What **services/products** was the customer inquiring about or facing issues with?
5. How did the agent respond to and resolve the issue throughout the call?
6. Was the **customer satisfied** at the end of the call?
7. Did the customer express any **emotions or sentiments** (positive, negative, or neutral)?
8. Were there any mentions of **competitors**, or any opportunities for **upselling or cross-selling**?
9. Summarize the **resolution** and whether it was successful.

Provide your answer in a clear, structured format with section headings and bullet points.
"""

SUMMARY_PROMPT_TEMPLATE = """
Based on this call analysis, summarize each of the following in 2–3 words:

{analysis_text}

1. Customer & Agent
2. Customer Type
3. Main Issue
4. Service Discussed
5. Agent's Response
6. Customer Satisfaction
7. Sentiment
8. Competitor or Upsell
9. Resolution
"""


class CallAnalytics:
    def __init__(self, client):
        self.client = client
        self.transcriptions = {}

    def process_audio_files(self, audio_paths: List[str]) -> Dict[str, str]:
        if not audio_paths:
            print("No audio files provided")
            return {}

        print(f"Processing {len(audio_paths)} audio files...")

        try:
            job = client.speech_to_text_translate_job.create_job(
                model="saaras:v3",
                with_diarization=True,
            )

            job.upload_files(file_paths=audio_paths, timeout=300)
            job.start()

            print("Waiting for transcription to complete...")
            job.wait_until_complete()

            if job.is_failed():
                print("Transcription failed!")
                return {}

            output_dir = Path(f"{OUTPUT_DIR}/transcriptions_{job.job_id}")
            output_dir.mkdir(parents=True, exist_ok=True)
            job.download_outputs(output_dir=str(output_dir))
            json_files = list(output_dir.glob("*.json"))
            if not json_files:
                raise FileNotFoundError(
                    f"No .json transcription files found in {output_dir}."
                )

            transcriptions = self._parse_transcriptions(output_dir)
            self.transcriptions.update(transcriptions)

            print(f"Successfully transcribed {len(transcriptions)} files!")

            for file_name, data in transcriptions.items():
                self.analyze_transcription(
                    data["conversation_path"], output_dir, file_name
                )

            return transcriptions

        except Exception as e:
            print(f"Error processing audio files: {e}")
            return {}

    def _parse_transcriptions(self, output_dir: Path) -> Dict[str, dict]:
        transcriptions = {}
        for json_file in output_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                diarized = data.get("diarized_transcript", {}).get("entries")
                speaker_times = {}
                lines = []
                if diarized:
                    for entry in diarized:
                        speaker = entry["speaker_id"]
                        text = entry["transcript"]
                        lines.append(f"{speaker}: {text}")
                        start = entry.get("start_time_seconds")
                        end = entry.get("end_time_seconds")
                        if start is not None and end is not None:
                            duration = end - start
                            speaker_times[speaker] = (
                                speaker_times.get(speaker, 0.0) + duration
                            )
                else:
                    lines = [f"UNKNOWN: {data.get('transcript', '')}"]

                conversation_text = "\n".join(lines)
                txt_path = output_dir / f"{json_file.stem}_conversation.txt"
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(conversation_text)

                timing_path = None
                if speaker_times:
                    timing_path = output_dir / f"{json_file.stem}_timing.json"
                    with open(timing_path, "w", encoding="utf-8") as f:
                        json.dump(speaker_times, f, indent=2)
                transcriptions[json_file.stem] = {
                    "entries": diarized or [],
                    "conversation_path": str(txt_path),
                    "timing_path": str(timing_path) if timing_path else None,
                }
            except Exception as e:
                print(f"Error parsing {json_file}: {e}")
        return transcriptions

    def analyze_transcription(
        self, conversation_path: str, output_dir: Path, file_name: str
    ) -> Dict:
        try:
            with open(conversation_path, "r", encoding="utf-8") as f:
                transcription = f.read()
            analysis_prompt = textwrap.dedent(
                ANALYSIS_PROMPT_TEMPLATE.format(transcription=transcription)
            )
            messages = [
                {
                    "role": "system",
                    "content": "You are a call analytics expert working for a company's support operations team. Your job is to understand customer calls end-to-end and provide structured insights to improve customer experience and agent effectiveness.",
                },
                {"role": "user", "content": analysis_prompt},
            ]
            response = self.client.chat.completions(messages=messages)
            analysis = response.choices[0].message.content
            analysis_path = output_dir / f"{file_name}_analysis.txt"
            with open(analysis_path, "w", encoding="utf-8") as f:
                f.write(analysis.strip())
            print(f"Analysis saved to {analysis_path}")
            return {"file_name": file_name, "analysis_path": str(analysis_path)}
        except Exception as e:
            error_msg = f"Error analyzing transcription: {str(e)}"
            print(error_msg)
            return {
                "file_name": file_name,
                "error": error_msg,
                "timestamp": datetime.now().isoformat(),
            }

    def answer_question(self, question: str, output_dir: Optional[Path] = None) -> None:
        for file_name, data in self.transcriptions.items():
            try:
                with open(data["conversation_path"], "r", encoding="utf-8") as f:
                    transcription = f.read()
                prompt = f"Based on this call transcription, answer the question below:\n\nTRANSCRIPTION:\n{transcription}\n\nQUESTION: {question}"
                messages = [
                    {"role": "system", "content": ""},
                    {"role": "user", "content": prompt},
                ]
                response = self.client.chat.completions(messages=messages)
                answer = response.choices[0].message.content
                q_hash = hashlib.sha1(question.encode()).hexdigest()[:6]
                path = (
                    Path(data["conversation_path"]).parent
                    / f"{file_name}_question_{q_hash}.txt"
                )
                with open(path, "w", encoding="utf-8") as f:
                    f.write(f"Question: {question}\n\nAnswer:\n{answer}")
                print(f"Answer saved to {path}")
            except Exception as e:
                print(f"Error answering question for {file_name}: {e}")

    def get_summary(self, output_dir: Optional[Path] = None) -> None:
        output_dir = output_dir or Path(OUTPUT_DIR)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = output_dir / f"summary_{timestamp}.txt"
        try:
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write("CALL ANALYTICS SUMMARY REPORT\n")
                f.write("=" * 60 + "\n")
                f.write(f"Generated: {datetime.now()}\n")
                f.write(f"Total Calls: {len(self.transcriptions)}\n")
                f.write("=" * 60 + "\n\n")
                for file_name, data in self.transcriptions.items():
                    analysis_file = (
                        Path(data["conversation_path"]).parent
                        / f"{file_name}_analysis.txt"
                    )
                    if not analysis_file.exists():
                        print(f"Analysis file not found for {file_name}, skipping.")
                        continue
                    with open(analysis_file, "r", encoding="utf-8") as af:
                        analysis_text = af.read()
                    summary_prompt = textwrap.dedent(
                        SUMMARY_PROMPT_TEMPLATE.format(analysis_text=analysis_text)
                    )
                    messages = [
                        {
                            "role": "system",
                            "content": "You are a call analytics summarizing expert. Provide concise and clear answers to each point ",
                        },
                        {"role": "user", "content": summary_prompt},
                    ]
                    response = self.client.chat.completions(messages=messages)
                    concise_summary = response.choices[0].message.content.strip()
                    f.write(f"Call File: {file_name}\n")
                    f.write("-" * 30 + "\n")
                    f.write(f"{concise_summary}\n\n")
            print(f"Summary saved to {summary_path}")
        except Exception as e:
            print(f"Error writing summary: {e}")


env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
client = SarvamAI(api_subscription_key=os.getenv("SARVAM_API_KEY"))
analytics = CallAnalytics(client=client)

audio_path = "C:/Hiral/Work/Si2 Resume Screener/resumer screener setup/call_transcribe/audio_files/1.mp3"
analytics.process_audio_files([audio_path])
analytics.answer_question("Give me the summary of the recording.")
analytics.get_summary()
