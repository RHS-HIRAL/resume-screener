/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 * 
 * npm run dev
 */

import React, { useState, useEffect } from 'react';
import { Mic, Upload, Sparkles, AlertTriangle, Moon, Sun, Cpu, Cloud, Zap, Server, Wifi, WifiOff, Download, FileText } from 'lucide-react';
import AudioRecorder from './components/AudioRecorder';
import FileUploader from './components/FileUploader';
import TranscriptionDisplay from './components/TranscriptionDisplay';
import Button from './components/Button';
import { transcribeAudio, checkHealth } from './services/transcriptionService';
import { AppStatus, AudioData, TranscriptionResponse, ProcessingMode } from './types';

const MODE_CONFIG: Record<ProcessingMode, {
  label: string;
  icon: React.ReactNode;
  description: string;
  color: string;
  activeColor: string;
  processingText: string;
}> = {
  gemini: {
    label: 'Gemini API',
    icon: <Sparkles size={16} />,
    description: 'Google Gemini — emotions, translations',
    color: 'text-indigo-600 dark:text-indigo-400',
    activeColor: 'bg-indigo-600',
    processingText: 'Gemini is detecting languages and generating your transcript...',
  },
  groq: {
    label: 'Groq API',
    icon: <Zap size={16} />,
    description: 'Groq Whisper — ultra-fast cloud transcription',
    color: 'text-orange-600 dark:text-orange-400',
    activeColor: 'bg-orange-600',
    processingText: 'Groq is processing your audio at lightning speed with Whisper Large V3...',
  },
};

function App() {
  const [inputMode, setInputMode] = useState<'record' | 'upload'>('record');
  const [processingMode, setProcessingMode] = useState<ProcessingMode>('gemini');
  const [status, setStatus] = useState<AppStatus>('idle');
  const [audioData, setAudioData] = useState<AudioData | null>(null);
  const [result, setResult] = useState<TranscriptionResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [backendOnline, setBackendOnline] = useState<boolean | null>(null);

  // Initialize dark mode based on system preference
  const [isDarkMode, setIsDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return false;
  });

  // Toggle Dark Mode
  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDarkMode]);

  // Check backend health on mount and when processing mode changes
  useEffect(() => {
    checkHealth()
      .then(() => setBackendOnline(true))
      .catch(() => setBackendOnline(false));
  }, []);

  const toggleDarkMode = () => setIsDarkMode(!isDarkMode);

  const handleAudioReady = (data: AudioData) => {
    setAudioData(data);
    setError(null);
    setResult(null);
  };

  const handleTranscribe = async () => {
    if (!audioData) return;

    setStatus('processing');
    setError(null);

    try {
      const data = await transcribeAudio(processingMode, audioData);
      setResult(data as TranscriptionResponse);
      setStatus('success');
    } catch (err: any) {
      console.error(err);
      setError(err.message || "An error occurred during transcription. Please try again.");
      setStatus('error');
    }
  };

  const handleReset = () => {
    setAudioData(null);
    setResult(null);
    setStatus('idle');
    setError(null);
  };

  const handleDownload = () => {
    if (!result) return;
    const dataStr = JSON.stringify(result, null, 2);
    const blob = new Blob([dataStr], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "transcription_data.json";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleDownloadTxt = () => {
    if (!result) return;
    
    let textContent = result.summary ? `Summary:\n${result.summary}\n\n` : '';
    textContent += 'Transcript:\n-------------------\n';
    
    result.segments.forEach(seg => {
      textContent += `[${seg.timestamp}] (${seg.language}): ${seg.content}\n`;
      if (seg.translation) {
        textContent += `Translation: ${seg.translation}\n`;
      }
      textContent += '\n';
    });

    const blob = new Blob([textContent], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "transcription.txt";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const currentModeConfig = MODE_CONFIG[processingMode];

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100 pb-20 transition-colors duration-300">
      {/* Header */}
      <header className="bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 sticky top-0 z-10 transition-colors duration-300">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <div className="bg-indigo-600 p-2 rounded-lg text-white shadow-lg shadow-indigo-500/30">
              <Sparkles size={20} />
            </div>
            <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-indigo-600 to-violet-600 dark:from-indigo-400 dark:to-violet-400">
              EchoScript AI
            </h1>
          </div>
          <div className="flex items-center space-x-4">
            {/* Backend Status Indicator */}
            <div className="flex items-center space-x-1.5 text-xs">
              {backendOnline === null ? (
                <span className="text-slate-400">Checking...</span>
              ) : backendOnline ? (
                <>
                  <Wifi size={14} className="text-emerald-500" />
                  <span className="text-emerald-600 dark:text-emerald-400 font-medium hidden sm:inline">Backend Online</span>
                </>
              ) : (
                <>
                  <WifiOff size={14} className="text-red-500" />
                  <span className="text-red-600 dark:text-red-400 font-medium hidden sm:inline">Backend Offline</span>
                </>
              )}
            </div>
            <button
              onClick={toggleDarkMode}
              className="p-2 text-slate-500 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500"
              aria-label="Toggle Dark Mode"
            >
              {isDarkMode ? <Sun size={20} /> : <Moon size={20} />}
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-3xl mx-auto px-4 sm:px-6 py-10">
        
        {/* Intro */}
        <div className="text-center mb-10">
          <h2 className="text-3xl font-bold text-slate-900 dark:text-white mb-4">
            Turn your audio into accurate text
          </h2>
          <p className="text-lg text-slate-600 dark:text-slate-400 max-w-2xl mx-auto">
            Upload a file or record directly. Choose between local, Gemini, or Groq processing for speaker-identified transcripts.
          </p>
        </div>

        {/* Processing Mode Selector */}
        {!result && (
          <div className="mb-8">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 uppercase tracking-wider flex items-center">
                <Server size={14} className="mr-1.5" />
                Processing Engine
              </h3>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {(Object.entries(MODE_CONFIG) as [ProcessingMode, typeof MODE_CONFIG['local']][]).map(([key, config]) => (
                <button
                  key={key}
                  onClick={() => { setProcessingMode(key); setError(null); }}
                  disabled={status === 'processing'}
                  className={`relative flex flex-col items-start p-4 rounded-xl border-2 transition-all text-left focus:outline-none focus:ring-2 focus:ring-offset-1 dark:focus:ring-offset-slate-950 focus:ring-indigo-500 ${
                    processingMode === key
                      ? `border-current ${config.color} bg-white dark:bg-slate-900 shadow-md`
                      : 'border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 hover:border-slate-300 dark:hover:border-slate-700 text-slate-600 dark:text-slate-400'
                  } ${status === 'processing' ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
                >
                  {processingMode === key && (
                    <div className={`absolute top-3 right-3 w-2.5 h-2.5 rounded-full ${config.activeColor} animate-pulse`} />
                  )}
                  <div className="flex items-center space-x-2 mb-2">
                    {config.icon}
                    <span className="font-semibold text-sm">{config.label}</span>
                  </div>
                  <p className="text-xs leading-relaxed text-slate-500 dark:text-slate-400">
                    {config.description}
                  </p>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Status Error */}
        {status === 'error' && error && (
          <div className="mb-6 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4 flex items-start text-red-700 dark:text-red-400">
            <AlertTriangle className="mr-3 flex-shrink-0 mt-0.5" size={20} />
            <p>{error}</p>
          </div>
        )}

        {/* Backend Offline Warning */}
        {backendOnline === false && (
          <div className="mb-6 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-xl p-4 flex items-start text-amber-700 dark:text-amber-400">
            <AlertTriangle className="mr-3 flex-shrink-0 mt-0.5" size={20} />
            <div>
              <p className="font-medium">Backend server is offline</p>
              <p className="text-sm mt-1">
                Start the backend with: <code className="bg-amber-100 dark:bg-amber-900/40 px-1.5 py-0.5 rounded text-xs">cd backend && python -m uvicorn main:app --reload --port 8000</code>
              </p>
            </div>
          </div>
        )}

        {/* Input Selection Tabs */}
        {!result && (
            <div className="bg-white dark:bg-slate-900 p-1 rounded-xl shadow-sm border border-slate-200 dark:border-slate-800 inline-flex mb-8 w-full sm:w-auto transition-colors duration-300">
            <button
                onClick={() => { setInputMode('record'); handleReset(); }}
                className={`flex-1 sm:flex-none flex items-center justify-center px-6 py-2.5 rounded-lg text-sm font-medium transition-all focus:outline-none focus:ring-2 focus:ring-offset-1 dark:focus:ring-offset-slate-900 focus:ring-indigo-500 ${
                inputMode === 'record' 
                    ? 'bg-indigo-600 text-white shadow-sm' 
                    : 'text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800'
                }`}
                disabled={status === 'processing'}
            >
                <Mic size={16} className="mr-2" />
                Record Audio
            </button>
            <button
                onClick={() => { setInputMode('upload'); handleReset(); }}
                className={`flex-1 sm:flex-none flex items-center justify-center px-6 py-2.5 rounded-lg text-sm font-medium transition-all focus:outline-none focus:ring-2 focus:ring-offset-1 dark:focus:ring-offset-slate-900 focus:ring-indigo-500 ${
                inputMode === 'upload' 
                    ? 'bg-indigo-600 text-white shadow-sm' 
                    : 'text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800'
                }`}
                disabled={status === 'processing'}
            >
                <Upload size={16} className="mr-2" />
                Upload File
            </button>
            </div>
        )}

        {/* Main Content Area */}
        <div className="space-y-8">
          
          {/* Input Section */}
          {!result && status !== 'processing' && (
            <div className="bg-white dark:bg-slate-900 rounded-2xl shadow-sm border border-slate-200 dark:border-slate-800 p-6 sm:p-8 transition-colors duration-300">
              {inputMode === 'record' ? (
                <AudioRecorder onAudioCaptured={handleAudioReady} disabled={status === 'processing'} />
              ) : (
                <FileUploader onFileSelected={handleAudioReady} disabled={status === 'processing'} />
              )}

              {audioData && (
                <div className="mt-6 flex justify-end pt-6 border-t border-slate-100 dark:border-slate-800">
                  <Button 
                    onClick={handleTranscribe} 
                    isLoading={status === 'processing'}
                    className="w-full sm:w-auto"
                    icon={currentModeConfig.icon}
                  >
                    Transcribe with {currentModeConfig.label}
                  </Button>
                </div>
              )}
            </div>
          )}

          {/* Processing State */}
          {status === 'processing' && (
            <div className="bg-white dark:bg-slate-900 rounded-2xl shadow-sm border border-slate-200 dark:border-slate-800 p-12 text-center transition-colors duration-300">
              <div className="flex justify-center mb-6">
                 <div className="relative">
                    <div className={`w-16 h-16 border-4 border-slate-100 dark:border-slate-800 rounded-full animate-spin ${
                      processingMode === 'gemini' ? 'border-t-indigo-600 dark:border-t-indigo-500' :
                      'border-t-orange-600 dark:border-t-orange-500'
                    }`}></div>
                    <div className="absolute top-0 left-0 w-full h-full flex items-center justify-center">
                        <span className={`animate-pulse ${currentModeConfig.color}`}>
                          {currentModeConfig.icon}
                        </span>
                    </div>
                 </div>
              </div>
              <h3 className="text-xl font-semibold text-slate-900 dark:text-white mb-2">
                Analyzing Audio...
              </h3>
              <p className="text-slate-500 dark:text-slate-400 max-w-sm mx-auto">
                {currentModeConfig.processingText}
              </p>
            </div>
          )}

          {/* Results Section */}
          {result && status === 'success' && (
            <div>
                <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 gap-4">
                    <div>
                      <h2 className="text-2xl font-bold text-slate-900 dark:text-white">Transcription Results</h2>
                      <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
                        Processed with {currentModeConfig.label}
                      </p>
                    </div>
                    <div className="flex flex-wrap items-center gap-3 w-full sm:w-auto">
                      <Button onClick={handleDownloadTxt} className="flex-1 sm:flex-none flex items-center justify-center bg-emerald-600 hover:bg-emerald-700 text-white">
                        <FileText size={16} className="mr-2" />
                        Save TXT
                      </Button>
                      <Button onClick={handleDownload} className="flex-1 sm:flex-none flex items-center justify-center bg-indigo-600 hover:bg-indigo-700 text-white">
                        <Download size={16} className="mr-2" />
                        Save JSON
                      </Button>
                      <Button onClick={handleReset} variant="secondary" className="flex-1 sm:flex-none">Start Over</Button>
                    </div>
                </div>

                {/* Audio Preview Player */}
                {audioData && (
                  <div className="mb-6 p-4 bg-white dark:bg-slate-900 rounded-xl shadow-sm border border-slate-200 dark:border-slate-800 transition-colors duration-300">
                    <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3 flex items-center">
                      <Mic size={16} className="mr-2 text-indigo-500" />
                      Audio Preview
                    </h3>
                    <audio 
                      controls 
                      className="w-full rounded-md outline-none" 
                      src={URL.createObjectURL(audioData.blob)} 
                    />
                  </div>
                )}

                <TranscriptionDisplay data={result} />
            </div>
          )}
        </div>

        {/* Disclaimer */}
        <div className="mt-16 text-center text-xs text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed border-t border-slate-200 dark:border-slate-800 pt-8 transition-colors duration-300">
            <p className="mb-2">
            By using this feature, you confirm that you have the necessary rights to any content that you upload. Do not upload content that infringes on others' intellectual property or privacy rights.
            </p>
            <p>
            Local processing keeps your data on-device. Cloud APIs (Gemini, Groq) send audio to third-party servers for processing.
            </p>
        </div>

      </main>
    </div>
  );
}

export default App;