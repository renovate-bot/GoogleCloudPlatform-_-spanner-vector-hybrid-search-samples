/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export interface SSEOptions {
  onMessage: (data: any) => void;
  onError?: (error: any) => void;
  onDone?: () => void;
}

export function streamEvents(url: string, options: SSEOptions): () => void {
  let isCancelled = false;
  const controller = new AbortController();

  const run = async () => {
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: 'text/event-stream',
        },
      });

      if (!response.ok) {
        throw new Error(`SSE stream failed with status ${response.status}`);
      }

      if (!response.body) {
        throw new Error('Response body is null');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      while (!isCancelled) {
        const { value, done } = await reader.read();
        if (done) {
          if (options.onDone) options.onDone();
          break;
        }

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (trimmed.startsWith('data: ')) {
            const rawData = trimmed.slice(6);
            try {
              const parsed = JSON.parse(rawData);
              options.onMessage(parsed);
              if (parsed.done && options.onDone) {
                options.onDone();
              }
            } catch {
              options.onMessage(rawData);
            }
          }
        }
      }
    } catch (err: any) {
      if (err.name !== 'AbortError' && !isCancelled) {
        if (options.onError) {
          options.onError(err);
        }
      }
    }
  };

  run();

  // Return cancel function
  return () => {
    isCancelled = true;
    controller.abort();
  };
}
