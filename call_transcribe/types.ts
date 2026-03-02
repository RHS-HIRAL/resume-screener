/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

export type ProcessingMode = 'gemini' | 'groq';

export enum Emotion {
  Happy = 'Happy',
  Sad = 'Sad',
  Angry = 'Angry',
  Neutral = 'Neutral'
}

export interface TranscriptionSegment {
  timestamp: string;
  content: string;
  language: string;
  language_code?: string;
  translation?: string;
  emotion?: Emotion;
}

export interface TranscriptionResponse {
  summary: string;
  segments: TranscriptionSegment[];
}

export type AppStatus = 'idle' | 'recording' | 'processing' | 'success' | 'error';

export interface AudioData {
  blob: Blob;
  base64: string;
  mimeType: string;
}