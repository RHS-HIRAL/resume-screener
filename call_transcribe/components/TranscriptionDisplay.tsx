/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
*/

import React from 'react';
import { TranscriptionResponse, Emotion } from '../types';
import { User, Clock, Globe, Languages, Smile, Frown, AlertCircle, Meh } from 'lucide-react';

interface TranscriptionDisplayProps {
  data: TranscriptionResponse;
}

const TranscriptionDisplay: React.FC<TranscriptionDisplayProps> = ({ data }) => {
  
  const getEmotionBadge = (emotion?: Emotion) => {
    if (!emotion) return null;

    switch (emotion) {
      case Emotion.Happy:
        return (
          <div className="flex items-center bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 px-2 py-1 rounded border border-green-100 dark:border-green-800">
            <Smile size={14} className="mr-1.5" />
            {emotion}
          </div>
        );
      case Emotion.Sad:
        return (
          <div className="flex items-center bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 px-2 py-1 rounded border border-blue-100 dark:border-blue-800">
            <Frown size={14} className="mr-1.5" />
            {emotion}
          </div>
        );
      case Emotion.Angry:
        return (
          <div className="flex items-center bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 px-2 py-1 rounded border border-red-100 dark:border-red-800">
            <AlertCircle size={14} className="mr-1.5" />
            {emotion}
          </div>
        );
      case Emotion.Neutral:
      default:
        return (
          <div className="flex items-center bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 px-2 py-1 rounded border border-slate-200 dark:border-slate-600">
            <Meh size={14} className="mr-1.5" />
            {emotion}
          </div>
        );
    }
  };

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
      
      {/* Summary Section */}
      <div className="bg-gradient-to-br from-indigo-50 to-white dark:from-slate-800 dark:to-slate-900 border border-indigo-100 dark:border-slate-700 rounded-2xl p-6 shadow-sm transition-colors duration-300">
        <h2 className="text-lg font-semibold text-indigo-900 dark:text-indigo-200 mb-3">Summary</h2>
        <p className="text-slate-700 dark:text-slate-300 leading-relaxed">{data.summary}</p>
      </div>

      {/* Segments Section */}
      <div className="space-y-4">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-white px-1">Detailed Transcript</h2>
        
        {data.segments.map((segment, index) => (
          <div 
            key={index} 
            className="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-5 hover:shadow-md transition-all duration-300"
          >
            <div className="flex flex-wrap items-center gap-3 mb-3 text-sm text-slate-500 dark:text-slate-400">
              <div className="flex items-center bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded">
                <Clock size={14} className="mr-1.5" />
                {segment.timestamp}
              </div>
              <div className="flex items-center bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded">
                <Globe size={14} className="mr-1.5" />
                {segment.language}
              </div>
              {segment.emotion && getEmotionBadge(segment.emotion)}
            </div>
            
            <p className="text-slate-800 dark:text-slate-200 leading-relaxed whitespace-pre-wrap">
              {segment.content}
            </p>

            {segment.translation && (
              <div className="mt-4 pt-3 border-t border-slate-100 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-900/30 -mx-5 -mb-5 px-5 pb-5 rounded-b-xl">
                 <div className="flex items-center text-xs font-semibold text-indigo-600 dark:text-indigo-300 mb-1.5 uppercase tracking-wide pt-2">
                    <Languages size={14} className="mr-1.5" />
                    English Translation
                 </div>
                 <p className="text-slate-600 dark:text-slate-400 italic leading-relaxed">
                   {segment.translation}
                 </p>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

export default TranscriptionDisplay;