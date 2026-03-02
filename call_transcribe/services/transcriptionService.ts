/**
 * Unified Transcription Service
 * Supports: Local Whisper, Gemini API, Groq API
 */

import { TranscriptionResponse, ProcessingMode } from '../types';

const API_BASE = '/api';

/**
 * Transcribe audio using the specified processing mode.
 */
export const transcribeAudio = async (
  mode: ProcessingMode,
  audioData: { blob: Blob; base64: string; mimeType: string }
): Promise<TranscriptionResponse> => {
  switch (mode) {
    case 'gemini':
      return transcribeGemini(audioData.base64, audioData.mimeType);
    case 'groq':
      return transcribeGroq(audioData.blob);
    default:
      throw new Error(`Unknown processing mode: ${mode}`);
  }
};

/**
 * Gemini API transcription via FastAPI backend.
 */
async function transcribeGemini(
  base64Audio: string,
  mimeType: string
): Promise<TranscriptionResponse> {
  const formData = new FormData();
  formData.append('audio_base64', base64Audio);
  formData.append('mime_type', mimeType);

  const response = await fetch(`${API_BASE}/transcribe/gemini`, {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(err.detail || `Gemini transcription failed (${response.status})`);
  }

  return response.json();
}

/**
 * Groq Whisper API transcription via FastAPI backend.
 */
async function transcribeGroq(blob: Blob): Promise<TranscriptionResponse> {
  const formData = new FormData();
  formData.append('file', blob, 'recording.webm');

  const response = await fetch(`${API_BASE}/transcribe/groq`, {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(err.detail || `Groq transcription failed (${response.status})`);
  }

  return response.json();
}

/**
 * Check backend health status.
 */
export const checkHealth = async () => {
  const response = await fetch(`${API_BASE}/health`);
  if (!response.ok) throw new Error('Backend unreachable');
  return response.json();
};
