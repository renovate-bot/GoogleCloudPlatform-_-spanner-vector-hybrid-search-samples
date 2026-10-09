// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Typography,
  Stack,
  IconButton,
  Button,
  TextField,
  Chip,
  Paper,
  CircularProgress,
  Tooltip,
  Divider,
  InputAdornment,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import SendIcon from '@mui/icons-material/Send';
import KeyIcon from '@mui/icons-material/VpnKey';
import SpeedIcon from '@mui/icons-material/Speed';
import CalculateIcon from '@mui/icons-material/Calculate';
import TuneIcon from '@mui/icons-material/Tune';
import MicIcon from '@mui/icons-material/Mic';
import StopIcon from '@mui/icons-material/Stop';
import RestartAltIcon from '@mui/icons-material/RestartAlt';

import { AiChatMessage, AiChatResponse } from '../types/ai';
import { sendAiChat, transcribeAudio } from '../services/api';

interface AiChatDrawerProps {
  open: boolean;
  onClose: () => void;
  hasKey: boolean;
  model?: string;
  onOpenKeyDialog: () => void;
  currentContext?: {
    selected_config?: string;
    nodes?: number;
    leader_region?: string;
    selected_clients?: string[];
  };
  onApplySizing: (configName: string, nodes: number) => void;
  onApplyBenchmark: (data: {
    spanner_config?: string;
    leader_region?: string;
    nodes?: number;
    client_regions?: string[];
    benchmark_name?: string;
    benchmark_description?: string;
    operations?: number;
    staleness_seconds?: number;
    optional_replicas?: string[];
  }) => void;
}

const INITIAL_GREETING: AiChatMessage = {
  id: 'init-1',
  role: 'assistant',
  content:
    "👋 Hello! I am your **Cloud Spanner Architecture & Benchmark Assistant (Experimental)**.\n\n" +
    "I can help you:\n" +
    "- **Design & configure benchmarks**: e.g., *'I want to measure latency for eur3 with clients in the leader and US read-only regions.'*\n" +
    "- **Calculate node sizing**: e.g., *'I need to be able to have 20k writes/sec in eur3.'*\n" +
    "- **Evaluate topologies**: e.g., *'What is the optimal configuration for German users requiring maximum availability?'*",
  timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
};

const QUICK_PROMPTS = [
  'I need to be able to have 20k writes/sec in eur3',
  'Measure latency in eur3 with clients in US RO regions and leader',
  'What is the optimal config for Germany with maximum availability?',
  'What can you do to help me?',
];

const STORAGE_KEY_MESSAGES = 'spanner_ai_chat_messages';
const STORAGE_KEY_ACTIONS = 'spanner_ai_chat_applied_actions';

const loadSavedMessages = (): AiChatMessage[] => {
  try {
    const saved = localStorage.getItem(STORAGE_KEY_MESSAGES);
    if (saved) {
      const parsed = JSON.parse(saved);
      if (Array.isArray(parsed) && parsed.length > 0) {
        return parsed;
      }
    }
  } catch (e) {
    console.warn('Could not restore AI chat messages:', e);
  }
  return [INITIAL_GREETING];
};

const loadSavedActions = (): Record<string, boolean> => {
  try {
    const saved = localStorage.getItem(STORAGE_KEY_ACTIONS);
    if (saved) {
      return JSON.parse(saved);
    }
  } catch (e) {
    console.warn('Could not restore AI applied actions:', e);
  }
  return {};
};

export const AiChatDrawer: React.FC<AiChatDrawerProps> = ({
  open,
  onClose,
  hasKey,
  model = 'gemini-3.8-flash',
  onOpenKeyDialog,
  currentContext,
  onApplySizing,
  onApplyBenchmark,
}) => {
  const [messages, setMessages] = useState<AiChatMessage[]>(loadSavedMessages);
  const [inputText, setInputText] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [appliedActions, setAppliedActions] = useState<Record<string, boolean>>(loadSavedActions);

  // Persist messages to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_MESSAGES, JSON.stringify(messages));
    } catch (e) {
      console.warn('Could not persist AI messages:', e);
    }
  }, [messages]);

  // Persist applied actions to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_ACTIONS, JSON.stringify(appliedActions));
    } catch (e) {
      console.warn('Could not persist AI actions:', e);
    }
  }, [appliedActions]);

  const handleResetChat = () => {
    setMessages([INITIAL_GREETING]);
    setAppliedActions({});
    setInputText('');
    try {
      localStorage.removeItem(STORAGE_KEY_MESSAGES);
      localStorage.removeItem(STORAGE_KEY_ACTIONS);
    } catch {
      // ignore
    }
  };

  // Audio Recording & Gemini Transcription State
  const [isRecording, setIsRecording] = useState<boolean>(false);
  const [recordingSeconds, setRecordingSeconds] = useState<number>(0);
  const [isTranscribing, setIsTranscribing] = useState<boolean>(false);
  const [transcribeError, setTranscribeError] = useState<string | null>(null);

  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const audioChunksRef = useRef<Blob[]>([]);
  const timerIntervalRef = useRef<any>(null);
  const streamRef = useRef<MediaStream | null>(null);

  // Clean up recording tracks on unmount or drawer close
  useEffect(() => {
    return () => {
      if (timerIntervalRef.current) clearInterval(timerIntervalRef.current);
      if (streamRef.current) {
        streamRef.current.getTracks().forEach((track) => track.stop());
      }
    };
  }, []);

  const startRecording = async () => {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      setTranscribeError('Microphone access is not supported by your browser.');
      return;
    }

    try {
      setTranscribeError(null);
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      streamRef.current = stream;

      let mimeType = 'audio/webm';
      if (typeof MediaRecorder.isTypeSupported === 'function') {
        if (MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
          mimeType = 'audio/webm;codecs=opus';
        } else if (MediaRecorder.isTypeSupported('audio/mp4')) {
          mimeType = 'audio/mp4';
        } else if (MediaRecorder.isTypeSupported('audio/ogg')) {
          mimeType = 'audio/ogg';
        }
      }

      const recorder = new MediaRecorder(stream, { mimeType });
      mediaRecorderRef.current = recorder;
      audioChunksRef.current = [];

      recorder.ondataavailable = (event) => {
        if (event.data && event.data.size > 0) {
          audioChunksRef.current.push(event.data);
        }
      };

      recorder.start(250);
      setIsRecording(true);
      setRecordingSeconds(0);

      if (timerIntervalRef.current) clearInterval(timerIntervalRef.current);
      timerIntervalRef.current = setInterval(() => {
        setRecordingSeconds((prev) => prev + 1);
      }, 1000);
    } catch (err: any) {
      console.error('Failed to start microphone recording:', err);
      setTranscribeError(
        err.name === 'NotAllowedError'
          ? 'Microphone permission denied. Please allow microphone access in your browser.'
          : err.message || 'Could not access microphone.'
      );
    }
  };

  const cancelRecording = () => {
    if (timerIntervalRef.current) clearInterval(timerIntervalRef.current);
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.onstop = null;
      mediaRecorderRef.current.stop();
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((track) => track.stop());
      streamRef.current = null;
    }
    audioChunksRef.current = [];
    setIsRecording(false);
    setRecordingSeconds(0);
  };

  const stopRecordingAndTranscribe = () => {
    if (timerIntervalRef.current) clearInterval(timerIntervalRef.current);

    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.onstop = async () => {
        const mime = mediaRecorderRef.current?.mimeType || 'audio/webm';
        const audioBlob = new Blob(audioChunksRef.current, { type: mime });

        if (streamRef.current) {
          streamRef.current.getTracks().forEach((track) => track.stop());
          streamRef.current = null;
        }

        if (audioBlob.size === 0) {
          setIsRecording(false);
          return;
        }

        setIsRecording(false);
        setIsTranscribing(true);

        try {
          const reader = new FileReader();
          reader.onloadend = async () => {
            const resultStr = reader.result as string;
            const base64Data = resultStr ? resultStr.split(',')[1] : null;
            if (!base64Data) {
              setIsTranscribing(false);
              return;
            }
            try {
              const res = await transcribeAudio(base64Data, mime);
              const text = res.text?.trim();
              if (text) {
                setInputText((prev) => (prev.trim() ? `${prev.trim()} ${text}` : text));
              }
            } catch (err: any) {
              console.error('Gemini transcription failed:', err);
              setTranscribeError(
                err?.response?.data?.detail || err.message || 'Gemini could not transcribe audio.'
              );
            } finally {
              setIsTranscribing(false);
            }
          };
          reader.readAsDataURL(audioBlob);
        } catch (e: any) {
          console.error('Error preparing audio for transcription:', e);
          setIsTranscribing(false);
        }
      };

      mediaRecorderRef.current.stop();
    } else {
      setIsRecording(false);
    }
  };

  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, open]);

  const handleSend = async (textToSend?: string) => {
    const prompt = (textToSend || inputText).trim();
    if (!prompt || loading) return;

    if (!hasKey) {
      onOpenKeyDialog();
      return;
    }

    const userMsg: AiChatMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: prompt,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    };

    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInputText('');
    setLoading(true);

    try {
      const historyPayload = newMessages.map((m) => ({
        role: m.role,
        content: m.content,
      }));

      const res: AiChatResponse = await sendAiChat(historyPayload, currentContext);

      const aiMsg: AiChatMessage = {
        id: `ai-${Date.now()}`,
        role: 'assistant',
        content: res.message || 'Configuration updated.',
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        action: res.action,
        spanner_config: res.spanner_config,
        leader_region: res.leader_region,
        nodes: res.nodes,
        client_regions: res.client_regions,
        benchmark_name: res.benchmark_name,
        benchmark_description: res.benchmark_description,
        operations: res.operations,
        staleness_seconds: res.staleness_seconds,
        optional_replicas: res.optional_replicas,
      };

      setMessages((prev) => [...prev, aiMsg]);

      // Automatically apply actions if unambiguous
      if (res.action === 'update_sizing' && res.nodes) {
        const targetCfg = res.spanner_config || currentContext?.selected_config || 'eur3';
        onApplySizing(targetCfg, res.nodes);
        if (res.optional_replicas !== undefined && res.spanner_config) {
          onApplyBenchmark({
            spanner_config: res.spanner_config,
            nodes: res.nodes,
            optional_replicas: res.optional_replicas,
          });
        }
        setAppliedActions((prev) => ({ ...prev, [aiMsg.id]: true }));
      } else if (res.action === 'configure_benchmark' || res.action === 'configure_and_size') {
        if (res.nodes && res.spanner_config) {
          onApplySizing(res.spanner_config, res.nodes);
        }
        onApplyBenchmark({
          spanner_config: res.spanner_config,
          leader_region: res.leader_region,
          nodes: res.nodes,
          client_regions: res.client_regions,
          benchmark_name: res.benchmark_name,
          benchmark_description: res.benchmark_description,
          operations: res.operations,
          staleness_seconds: res.staleness_seconds,
          optional_replicas: res.optional_replicas,
        });
        setAppliedActions((prev) => ({ ...prev, [aiMsg.id]: true }));
      }
    } catch (err: any) {
      console.error('AI chat failed:', err);
      const errorMsg: AiChatMessage = {
        id: `error-${Date.now()}`,
        role: 'assistant',
        content: `⚠️ ${err?.response?.data?.detail || err.message || 'Could not communicate with Gemini AI Assistant.'}`,
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      };
      setMessages((prev) => [...prev, errorMsg]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // Parse inline markdown tokens: bold, italic, code, links
  const renderInline = (text: string, isUser: boolean, keyPrefix = ''): React.ReactNode[] => {
    if (!text) return [];

    // Match links, inline code, bold-italic, bold, italic
    const tokenRegex = /(\[[^\]]+\]\([^)]+\)|`[^`\n]+`|\*\*\*[^*]+\*\*\*|\*\*[^*]+\*\*|\*[^*]+\*)/g;
    const parts = text.split(tokenRegex);

    return parts.map((part, index) => {
      const key = `${keyPrefix}-${index}`;
      if (!part) return null;

      // Link: [label](url)
      const linkMatch = part.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
      if (linkMatch) {
        return (
          <Box
            key={key}
            component="a"
            href={linkMatch[2]}
            target="_blank"
            rel="noopener noreferrer"
            sx={{
              color: isUser ? '#ffffff' : '#7c3aed',
              textDecoration: 'underline',
              fontWeight: 600,
              '&:hover': { opacity: 0.8 },
            }}
          >
            {linkMatch[1]}
          </Box>
        );
      }

      // Inline Code: `code`
      if (part.startsWith('`') && part.endsWith('`') && part.length >= 2) {
        const code = part.slice(1, -1);
        return (
          <Box
            key={key}
            component="code"
            sx={{
              fontFamily: 'SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
              fontSize: '0.82em',
              px: 0.6,
              py: 0.15,
              borderRadius: '4px',
              backgroundColor: isUser ? 'rgba(255,255,255,0.2)' : '#e2e8f0',
              color: isUser ? '#ffffff' : '#0f172a',
              border: isUser ? '1px solid rgba(255,255,255,0.3)' : '1px solid #cbd5e1',
              fontWeight: 600,
              whiteSpace: 'nowrap',
            }}
          >
            {code}
          </Box>
        );
      }

      // Bold + Italic: ***text***
      if (part.startsWith('***') && part.endsWith('***') && part.length >= 6) {
        return (
          <strong key={key} style={{ fontWeight: 700 }}>
            <em>{renderInline(part.slice(3, -3), isUser, `${key}-bi`)}</em>
          </strong>
        );
      }

      // Bold: **text**
      if (part.startsWith('**') && part.endsWith('**') && part.length >= 4) {
        return (
          <strong key={key} style={{ fontWeight: 700 }}>
            {renderInline(part.slice(2, -2), isUser, `${key}-b`)}
          </strong>
        );
      }

      // Italic: *text*
      if (part.startsWith('*') && part.endsWith('*') && part.length >= 2) {
        return (
          <em key={key} style={{ fontStyle: 'italic' }}>
            {renderInline(part.slice(1, -1), isUser, `${key}-i`)}
          </em>
        );
      }

      return part;
    });
  };

  // Convert raw LaTeX math symbols and expressions into clean readable text
  const cleanLatexMath = (text: string): string => {
    if (!text) return '';
    return text
      .replace(/\\text\{([^}]+)\}/g, '$1')
      .replace(/\\mathbf\{([^}]+)\}/g, '**$1**')
      .replace(/\\mathit\{([^}]+)\}/g, '*$1*')
      .replace(/\\mathrm\{([^}]+)\}/g, '$1')
      .replace(/\\div/g, '÷')
      .replace(/\\times/g, '×')
      .replace(/\\cdot/g, '·')
      .replace(/\\implies/g, '→')
      .replace(/\\rightarrow/g, '→')
      .replace(/\\approx/g, '≈')
      .replace(/\\leq/g, '≤')
      .replace(/\\le/g, '≤')
      .replace(/\\geq/g, '≥')
      .replace(/\\ge/g, '≥')
      .replace(/\\lceil/g, '⌈')
      .replace(/\\rceil/g, '⌉')
      .replace(/\$([^$]+)\$/g, '$1')
      .replace(/\\[a-zA-Z]+/g, '')
      .replace(/[ \t]{2,}/g, ' ');
  };

  // Render markdown text including blocks (headings, bullet points, numbers, code blocks, paragraphs)
  const renderMessageContent = (content: string, isUser: boolean) => {
    const cleanedContent = cleanLatexMath(content);
    const lines = cleanedContent.split('\n');
    const elements: React.ReactNode[] = [];
    let inCodeBlock = false;
    let codeBlockLines: string[] = [];

    const flushCodeBlock = (key: string | number) => {
      if (codeBlockLines.length > 0) {
        elements.push(
          <Box
            key={key}
            component="pre"
            sx={{
              backgroundColor: isUser ? 'rgba(0,0,0,0.25)' : '#0f172a',
              color: isUser ? '#ffffff' : '#f8fafc',
              p: 1.2,
              borderRadius: 1.5,
              overflowX: 'auto',
              fontSize: '0.78rem',
              fontFamily: 'SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
              my: 0.8,
              lineHeight: 1.45,
            }}
          >
            <code>{codeBlockLines.join('\n')}</code>
          </Box>
        );
        codeBlockLines = [];
      }
    };

    lines.forEach((line, idx) => {
      // Code fence toggles
      if (line.trim().startsWith('```')) {
        if (inCodeBlock) {
          flushCodeBlock(`codeblock-${idx}`);
          inCodeBlock = false;
        } else {
          inCodeBlock = true;
          codeBlockLines = [];
        }
        return;
      }

      if (inCodeBlock) {
        codeBlockLines.push(line);
        return;
      }

      // Empty line / paragraph separator
      if (line.trim() === '') {
        elements.push(<Box key={`empty-${idx}`} sx={{ height: 6 }} />);
        return;
      }

      // Headings: #, ##, ###, ####
      const headingMatch = line.match(/^(#{1,4})\s+(.*)$/);
      if (headingMatch) {
        const level = headingMatch[1].length;
        const headingText = headingMatch[2];
        const fontSizes: Record<number, string> = {
          1: '1.05rem',
          2: '0.98rem',
          3: '0.90rem',
          4: '0.86rem',
        };
        elements.push(
          <Typography
            key={`heading-${idx}`}
            variant="subtitle2"
            component="div"
            sx={{
              fontWeight: 700,
              fontSize: fontSizes[level] || '0.90rem',
              mt: idx === 0 ? 0.2 : 1.2,
              mb: 0.4,
              color: isUser ? '#ffffff' : '#0f172a',
              lineHeight: 1.3,
            }}
          >
            {renderInline(headingText, isUser, `h-${idx}`)}
          </Typography>
        );
        return;
      }

      // Bullet List Item: -, *, +
      const bulletMatch = line.match(/^(\s*)([-*+])\s+(.*)$/);
      if (bulletMatch) {
        const indentLevel = Math.min(4, Math.floor(bulletMatch[1].length / 2));
        const itemText = bulletMatch[3];
        elements.push(
          <Box
            key={`bullet-${idx}`}
            sx={{
              display: 'flex',
              alignItems: 'flex-start',
              gap: 1,
              pl: indentLevel * 1.5,
              mb: 0.35,
              lineHeight: 1.5,
            }}
          >
            <Box
              component="span"
              sx={{
                color: isUser ? 'rgba(255,255,255,0.7)' : '#7c3aed',
                fontWeight: 900,
                fontSize: '1rem',
                lineHeight: '1.3',
                userSelect: 'none',
                flexShrink: 0,
              }}
            >
              •
            </Box>
            <Box
              sx={{
                fontSize: '0.85rem',
                lineHeight: 1.55,
                color: isUser ? '#ffffff' : '#1e293b',
                flex: 1,
              }}
            >
              {renderInline(itemText, isUser, `b-${idx}`)}
            </Box>
          </Box>
        );
        return;
      }

      // Numbered List Item: 1. , 2. , etc.
      const numberMatch = line.match(/^(\s*)(\d+)\.\s+(.*)$/);
      if (numberMatch) {
        const indentLevel = Math.min(4, Math.floor(numberMatch[1].length / 2));
        const num = numberMatch[2];
        const itemText = numberMatch[3];
        elements.push(
          <Box
            key={`num-${idx}`}
            sx={{
              display: 'flex',
              alignItems: 'flex-start',
              gap: 0.8,
              pl: indentLevel * 1.5,
              mb: 0.35,
              lineHeight: 1.5,
            }}
          >
            <Box
              component="span"
              sx={{
                color: isUser ? 'rgba(255,255,255,0.8)' : '#64748b',
                fontWeight: 700,
                fontSize: '0.82rem',
                lineHeight: '1.55',
                userSelect: 'none',
                flexShrink: 0,
                minWidth: '1.2rem',
              }}
            >
              {num}.
            </Box>
            <Box
              sx={{
                fontSize: '0.85rem',
                lineHeight: 1.55,
                color: isUser ? '#ffffff' : '#1e293b',
                flex: 1,
              }}
            >
              {renderInline(itemText, isUser, `num-t-${idx}`)}
            </Box>
          </Box>
        );
        return;
      }

      // Blockquote: > text
      const quoteMatch = line.match(/^>\s*(.*)$/);
      if (quoteMatch) {
        elements.push(
          <Box
            key={`quote-${idx}`}
            sx={{
              borderLeft: isUser ? '3px solid rgba(255,255,255,0.6)' : '3px solid #7c3aed',
              pl: 1.2,
              py: 0.2,
              my: 0.5,
              fontStyle: 'italic',
              color: isUser ? 'rgba(255,255,255,0.9)' : '#475569',
              fontSize: '0.84rem',
              lineHeight: 1.5,
            }}
          >
            {renderInline(quoteMatch[1], isUser, `q-${idx}`)}
          </Box>
        );
        return;
      }

      // Standard Paragraph
      elements.push(
        <Typography
          key={`para-${idx}`}
          variant="body2"
          component="div"
          sx={{
            lineHeight: 1.55,
            mb: 0.4,
            color: isUser ? '#ffffff' : '#1e293b',
            fontSize: '0.85rem',
          }}
        >
          {renderInline(line, isUser, `p-${idx}`)}
        </Typography>
      );
    });

    if (inCodeBlock && codeBlockLines.length > 0) {
      flushCodeBlock('codeblock-final');
    }

    return elements;
  };

  return (
    <Box
      sx={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        backgroundColor: '#ffffff',
      }}
    >
      {/* Header */}
      <Box
        sx={{
          p: 1.5,
          px: 2,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          borderBottom: '1px solid #e2e8f0',
          backgroundColor: '#faf5ff',
        }}
      >
        <Stack direction="row" spacing={1.2} alignItems="center">
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 32,
              height: 32,
              borderRadius: '8px',
              background: 'linear-gradient(135deg, #7c3aed 0%, #3b82f6 100%)',
              color: '#ffffff',
            }}
          >
            <AutoAwesomeIcon sx={{ fontSize: 18 }} />
          </Box>
          <Box>
            <Stack direction="row" spacing={0.75} alignItems="center">
              <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#1e1b4b', fontSize: '0.9rem', lineHeight: 1.2 }}>
                Spanner AI Assistant
              </Typography>
              <Chip
                label="Experimental"
                size="small"
                sx={{
                  height: 16,
                  fontSize: '0.62rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  backgroundColor: '#fef3c7',
                  color: '#b45309',
                  border: '1px solid #fde68a',
                }}
              />
            </Stack>
            <Typography variant="caption" sx={{ color: '#7c3aed', fontWeight: 600, fontSize: '0.7rem' }}>
              {model}
            </Typography>
          </Box>
        </Stack>

        <Stack direction="row" spacing={0.5} alignItems="center">
          {messages.length > 1 && (
            <Tooltip title="Start new conversation (Reset chat)">
              <IconButton
                size="small"
                onClick={handleResetChat}
                sx={{
                  color: '#64748b',
                  '&:hover': { color: '#0f172a', backgroundColor: '#f1f5f9' },
                }}
              >
                <RestartAltIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          )}
          <Tooltip title={hasKey ? 'Gemini Key Configured (gemini.key)' : 'Activate Gemini API Key'}>
            <IconButton size="small" onClick={onOpenKeyDialog} sx={{ color: hasKey ? '#16a34a' : '#d97706' }}>
              <KeyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <IconButton size="small" onClick={onClose}>
            <CloseIcon fontSize="small" />
          </IconButton>
        </Stack>
      </Box>

      {/* API Key Missing Notification Banner */}
      {!hasKey && (
        <Box sx={{ p: 1.5, backgroundColor: '#fef3c7', borderBottom: '1px solid #fde68a', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Typography variant="caption" sx={{ color: '#92400e', fontWeight: 500, fontSize: '0.75rem' }}>
            AI Assistant is inactive. Enter your Gemini API key in <strong>gemini.key</strong> or via the UI to activate.
          </Typography>
          <Button
            size="small"
            variant="contained"
            onClick={onOpenKeyDialog}
            sx={{
              textTransform: 'none',
              fontSize: '0.7rem',
              py: 0.2,
              px: 1,
              backgroundColor: '#d97706',
              '&:hover': { backgroundColor: '#b45309' },
            }}
          >
            Activate Key
          </Button>
        </Box>
      )}

      {/* Messages List Area */}
      <Box sx={{ flexGrow: 1, overflowY: 'auto', p: 2, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
        {messages.map((m) => {
          const isUser = m.role === 'user';
          return (
            <Box
              key={m.id}
              sx={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: isUser ? 'flex-end' : 'flex-start',
                maxWidth: '100%',
              }}
            >
              <Paper
                elevation={0}
                sx={{
                  p: 1.5,
                  maxWidth: '88%',
                  borderRadius: isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
                  backgroundColor: isUser ? '#7c3aed' : '#f8fafc',
                  color: isUser ? '#ffffff' : '#1e293b',
                  border: isUser ? 'none' : '1px solid #e2e8f0',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                }}
              >
                {renderMessageContent(m.content, isUser)}

                {/* Sizing Action Card */}
                {(m.action === 'update_sizing' || (m.nodes && m.action !== 'configure_benchmark')) && m.nodes && (
                  <Paper
                    elevation={0}
                    sx={{
                      mt: 1.2,
                      p: 1.2,
                      backgroundColor: '#ffffff',
                      border: '1px solid #c084fc',
                      borderRadius: 1.5,
                      color: '#1e293b',
                    }}
                  >
                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 0.5 }}>
                      <CalculateIcon sx={{ color: '#7c3aed', fontSize: 18 }} />
                      <Typography variant="caption" sx={{ fontWeight: 700, color: '#7c3aed' }}>
                        Automated Sizing Calculation
                      </Typography>
                    </Stack>
                    <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
                      Recommended: {m.nodes} Nodes {m.spanner_config ? `(${m.spanner_config})` : ''}
                    </Typography>
                    <Button
                      size="small"
                      variant="outlined"
                      startIcon={<TuneIcon sx={{ fontSize: 14 }} />}
                      onClick={() => onApplySizing(m.spanner_config || currentContext?.selected_config || 'eur3', m.nodes!)}
                      sx={{
                        mt: 0.8,
                        textTransform: 'none',
                        fontSize: '0.72rem',
                        borderColor: '#7c3aed',
                        color: '#7c3aed',
                      }}
                    >
                      {appliedActions[m.id] ? 'Applied to Calculator ✓' : 'Apply to Sizing Table'}
                    </Button>
                  </Paper>
                )}

                {/* Benchmark Configuration Action Card */}
                {(m.action === 'configure_benchmark' || m.action === 'configure_and_size') && (
                  <Paper
                    elevation={0}
                    sx={{
                      mt: 1.2,
                      p: 1.2,
                      backgroundColor: '#ffffff',
                      border: '1px solid #38bdf8',
                      borderRadius: 1.5,
                      color: '#1e293b',
                    }}
                  >
                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 0.5 }}>
                      <SpeedIcon sx={{ color: '#0284c7', fontSize: 18 }} />
                      <Typography variant="caption" sx={{ fontWeight: 700, color: '#0284c7' }}>
                        Benchmark Scenario Ready
                      </Typography>
                    </Stack>
                    <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary' }}>
                      <strong>Config:</strong> {m.spanner_config || 'eur3'} {m.leader_region ? `(Leader: ${m.leader_region})` : ''}
                    </Typography>
                    {m.client_regions && m.client_regions.length > 0 && (
                      <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary', mt: 0.2 }}>
                        <strong>Clients ({m.client_regions.length}):</strong> {m.client_regions.join(', ')}
                      </Typography>
                    )}
                    {m.optional_replicas !== undefined && (
                      <Typography variant="caption" sx={{ display: 'block', color: '#0369a1', mt: 0.2, fontWeight: 500 }}>
                        <strong>Optional Replicas:</strong>{' '}
                        {m.optional_replicas.length > 0
                          ? `${m.optional_replicas.join(', ')} (${m.optional_replicas.length} active, others pruned)`
                          : 'None (all optional replicas deselected)'}
                      </Typography>
                    )}
                    <Button
                      size="small"
                      variant="contained"
                      startIcon={<SpeedIcon sx={{ fontSize: 14 }} />}
                      onClick={() => {
                        if (m.nodes && m.spanner_config) {
                          onApplySizing(m.spanner_config, m.nodes);
                        }
                        onApplyBenchmark({
                          spanner_config: m.spanner_config,
                          leader_region: m.leader_region,
                          nodes: m.nodes,
                          client_regions: m.client_regions,
                          benchmark_name: m.benchmark_name,
                          benchmark_description: m.benchmark_description,
                          operations: m.operations,
                          staleness_seconds: m.staleness_seconds,
                          optional_replicas: m.optional_replicas,
                        });
                        setAppliedActions((prev) => ({ ...prev, [m.id]: true }));
                      }}
                      sx={{
                        mt: 0.8,
                        textTransform: 'none',
                        fontSize: '0.72rem',
                        backgroundColor: '#0284c7',
                        '&:hover': { backgroundColor: '#0369a1' },
                      }}
                    >
                      {appliedActions[m.id] ? 'Active in Benchmark Form ✓' : 'Review in Benchmark Form'}
                    </Button>
                  </Paper>
                )}
              </Paper>
              <Typography variant="caption" sx={{ color: '#94a3b8', fontSize: '0.65rem', mt: 0.3, px: 0.5 }}>
                {m.timestamp}
              </Typography>
            </Box>
          );
        })}
        {loading && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 1 }}>
            <CircularProgress size={16} sx={{ color: '#7c3aed' }} />
            <Typography variant="caption" sx={{ color: '#7c3aed', fontStyle: 'italic', fontWeight: 500 }}>
              Analyzing Spanner topology & formulating configuration...
            </Typography>
          </Box>
        )}
        <div ref={messagesEndRef} />
      </Box>

      {/* Quick Prompts Bar */}
      <Box sx={{ px: 2, py: 1, backgroundColor: '#f8fafc', borderTop: '1px solid #f1f5f9' }}>
        <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 600, display: 'block', mb: 0.5, fontSize: '0.7rem' }}>
          Suggested Scenarios:
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.6 }}>
          {QUICK_PROMPTS.map((prompt, pIdx) => (
            <Chip
              key={pIdx}
              label={prompt}
              size="small"
              onClick={() => handleSend(prompt)}
              disabled={loading}
              sx={{
                fontSize: '0.7rem',
                backgroundColor: '#ffffff',
                border: '1px solid #cbd5e1',
                cursor: 'pointer',
                '&:hover': { backgroundColor: '#f1f5f9', borderColor: '#94a3b8' },
              }}
            />
          ))}
        </Box>
      </Box>

      <Divider />

      {/* Input Composer */}
      <Box sx={{ p: 1.5, px: 2, backgroundColor: '#ffffff' }}>
        {isRecording ? (
          /* Active Recording Bar */
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              p: 1.2,
              px: 2,
              borderRadius: 2,
              backgroundColor: '#fef2f2',
              border: '1px solid #fecaca',
            }}
          >
            <Stack direction="row" spacing={1.5} alignItems="center">
              <Box
                sx={{
                  width: 12,
                  height: 12,
                  borderRadius: '50%',
                  backgroundColor: '#ef4444',
                  animation: 'pulse-dot 1.2s infinite',
                  '@keyframes pulse-dot': {
                    '0%': { transform: 'scale(0.95)', opacity: 0.8 },
                    '50%': { transform: 'scale(1.3)', opacity: 1 },
                    '100%': { transform: 'scale(0.95)', opacity: 0.8 },
                  },
                }}
              />
              <Typography variant="body2" sx={{ fontWeight: 600, color: '#991b1b', fontSize: '0.85rem' }}>
                Recording... {Math.floor(recordingSeconds / 60)}:{String(recordingSeconds % 60).padStart(2, '0')}
              </Typography>
            </Stack>

            <Stack direction="row" spacing={1}>
              <Button
                size="small"
                variant="outlined"
                color="inherit"
                onClick={cancelRecording}
                sx={{
                  textTransform: 'none',
                  fontSize: '0.75rem',
                  borderColor: '#fca5a5',
                  color: '#b91c1c',
                }}
              >
                Cancel
              </Button>
              <Button
                size="small"
                variant="contained"
                startIcon={<StopIcon sx={{ fontSize: 16 }} />}
                onClick={stopRecordingAndTranscribe}
                sx={{
                  textTransform: 'none',
                  fontSize: '0.75rem',
                  backgroundColor: '#ef4444',
                  '&:hover': { backgroundColor: '#dc2626' },
                }}
              >
                Stop & Transcribe
              </Button>
            </Stack>
          </Box>
        ) : (
          /* Normal Text Input with Mic Adornment */
          <Stack direction="row" spacing={1} alignItems="flex-end">
            <TextField
              fullWidth
              size="small"
              multiline
              maxRows={4}
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={
                isTranscribing
                  ? 'Transcribing audio with Gemini...'
                  : hasKey
                  ? 'Type or click mic to dictate...'
                  : 'Activate Gemini Key to chat...'
              }
              disabled={loading || isTranscribing}
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    {isTranscribing ? (
                      <CircularProgress size={18} sx={{ color: '#7c3aed', mr: 0.5 }} />
                    ) : (
                      <Tooltip title={hasKey ? "Dictate with Gemini" : "Activate Gemini Key to dictate"}>
                        <span>
                          <IconButton
                            size="small"
                            onClick={startRecording}
                            disabled={loading || !hasKey}
                            sx={{
                              color: '#7c3aed',
                              p: 0.5,
                              mr: 0.2,
                              '&:hover': { backgroundColor: '#f3e8ff' },
                              '&.Mui-disabled': { color: '#cbd5e1' },
                            }}
                          >
                            <MicIcon sx={{ fontSize: 20 }} />
                          </IconButton>
                        </span>
                      </Tooltip>
                    )}
                  </InputAdornment>
                ),
              }}
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                  fontSize: '0.85rem',
                  backgroundColor: '#f8fafc',
                },
              }}
            />
            <IconButton
              color="primary"
              onClick={() => handleSend()}
              disabled={loading || isTranscribing || !inputText.trim()}
              sx={{
                backgroundColor: '#7c3aed',
                color: '#ffffff',
                borderRadius: 2,
                '&:hover': { backgroundColor: '#6d28d9' },
                '&.Mui-disabled': { backgroundColor: '#e2e8f0', color: '#94a3b8' },
              }}
            >
              {loading ? <CircularProgress size={18} color="inherit" /> : <SendIcon fontSize="small" />}
            </IconButton>
          </Stack>
        )}
        {transcribeError && (
          <Typography variant="caption" sx={{ color: '#ef4444', display: 'block', mt: 0.5, fontSize: '0.72rem' }}>
            ⚠️ {transcribeError}
          </Typography>
        )}
        <Typography
          variant="caption"
          sx={{
            display: 'block',
            textAlign: 'center',
            color: '#64748b',
            fontSize: '0.68rem',
            mt: 0.75,
            lineHeight: 1.3,
          }}
        >
          AI Assistant is experimental. Latency descriptions are qualitative architectural estimates; measure empirical performance via benchmarks.
        </Typography>
      </Box>
    </Box>
  );
};
