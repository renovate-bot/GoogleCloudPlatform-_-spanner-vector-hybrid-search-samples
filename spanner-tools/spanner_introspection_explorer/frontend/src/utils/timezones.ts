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

export interface TimezoneDesignation {
  code: string;
  name: string;
  offset: string;
  region: 'UTC & Europe' | 'Americas' | 'Asia & Pacific' | 'Middle East & Africa';
}

export const TIMEZONE_DESIGNATIONS: TimezoneDesignation[] = [
  // UTC & Europe
  { code: 'UTC', name: 'Coordinated Universal Time', offset: 'UTC +00:00', region: 'UTC & Europe' },
  { code: 'GMT', name: 'Greenwich Mean Time', offset: 'UTC +00:00', region: 'UTC & Europe' },
  { code: 'BST', name: 'British Summer Time', offset: 'UTC +01:00', region: 'UTC & Europe' },
  { code: 'CET', name: 'Central European Time', offset: 'UTC +01:00', region: 'UTC & Europe' },
  { code: 'CEST', name: 'Central European Summer Time', offset: 'UTC +02:00', region: 'UTC & Europe' },
  { code: 'WET', name: 'Western European Time', offset: 'UTC +00:00', region: 'UTC & Europe' },
  { code: 'WEST', name: 'Western European Summer Time', offset: 'UTC +01:00', region: 'UTC & Europe' },
  { code: 'EET', name: 'Eastern European Time', offset: 'UTC +02:00', region: 'UTC & Europe' },
  { code: 'EEST', name: 'Eastern European Summer Time', offset: 'UTC +03:00', region: 'UTC & Europe' },
  { code: 'MSK', name: 'Moscow Standard Time', offset: 'UTC +03:00', region: 'UTC & Europe' },

  // Americas
  { code: 'EDT', name: 'Eastern Daylight Time (US)', offset: 'UTC -04:00', region: 'Americas' },
  { code: 'EST', name: 'Eastern Standard Time (US)', offset: 'UTC -05:00', region: 'Americas' },
  { code: 'CDT', name: 'Central Daylight Time (US)', offset: 'UTC -05:00', region: 'Americas' },
  { code: 'CST', name: 'Central Standard Time (US)', offset: 'UTC -06:00', region: 'Americas' },
  { code: 'MDT', name: 'Mountain Daylight Time (US)', offset: 'UTC -06:00', region: 'Americas' },
  { code: 'MST', name: 'Mountain Standard Time (US)', offset: 'UTC -07:00', region: 'Americas' },
  { code: 'PDT', name: 'Pacific Daylight Time (US)', offset: 'UTC -07:00', region: 'Americas' },
  { code: 'PST', name: 'Pacific Standard Time (US)', offset: 'UTC -08:00', region: 'Americas' },
  { code: 'AKDT', name: 'Alaska Daylight Time', offset: 'UTC -08:00', region: 'Americas' },
  { code: 'AKST', name: 'Alaska Standard Time', offset: 'UTC -09:00', region: 'Americas' },
  { code: 'HST', name: 'Hawaii Standard Time', offset: 'UTC -10:00', region: 'Americas' },
  { code: 'AST', name: 'Atlantic Standard Time', offset: 'UTC -04:00', region: 'Americas' },
  { code: 'ADT', name: 'Atlantic Daylight Time', offset: 'UTC -03:00', region: 'Americas' },
  { code: 'NST', name: 'Newfoundland Standard Time', offset: 'UTC -03:30', region: 'Americas' },
  { code: 'NDT', name: 'Newfoundland Daylight Time', offset: 'UTC -02:30', region: 'Americas' },
  { code: 'BRT', name: 'Brasília Time', offset: 'UTC -03:00', region: 'Americas' },

  // Asia & Pacific
  { code: 'IST', name: 'India Standard Time', offset: 'UTC +05:30', region: 'Asia & Pacific' },
  { code: 'NPT', name: 'Nepal Time', offset: 'UTC +05:45', region: 'Asia & Pacific' },
  { code: 'ICT', name: 'Indochina Time (Bangkok, Jakarta)', offset: 'UTC +07:00', region: 'Asia & Pacific' },
  { code: 'CST (Asia)', name: 'China Standard Time', offset: 'UTC +08:00', region: 'Asia & Pacific' },
  { code: 'SGT', name: 'Singapore Standard Time', offset: 'UTC +08:00', region: 'Asia & Pacific' },
  { code: 'HKT', name: 'Hong Kong Time', offset: 'UTC +08:00', region: 'Asia & Pacific' },
  { code: 'JST', name: 'Japan Standard Time', offset: 'UTC +09:00', region: 'Asia & Pacific' },
  { code: 'KST', name: 'Korea Standard Time', offset: 'UTC +09:00', region: 'Asia & Pacific' },
  { code: 'ACST', name: 'Australian Central Standard Time', offset: 'UTC +09:30', region: 'Asia & Pacific' },
  { code: 'ACDT', name: 'Australian Central Daylight Time', offset: 'UTC +10:30', region: 'Asia & Pacific' },
  { code: 'AEST', name: 'Australian Eastern Standard Time', offset: 'UTC +10:00', region: 'Asia & Pacific' },
  { code: 'AEDT', name: 'Australian Eastern Daylight Time', offset: 'UTC +11:00', region: 'Asia & Pacific' },
  { code: 'NZST', name: 'New Zealand Standard Time', offset: 'UTC +12:00', region: 'Asia & Pacific' },
  { code: 'NZDT', name: 'New Zealand Daylight Time', offset: 'UTC +13:00', region: 'Asia & Pacific' },

  // Middle East & Africa
  { code: 'GST', name: 'Gulf Standard Time (Dubai)', offset: 'UTC +04:00', region: 'Middle East & Africa' },
  { code: 'AST (Arabia)', name: 'Arabia Standard Time (Riyadh)', offset: 'UTC +03:00', region: 'Middle East & Africa' },
  { code: 'CAT', name: 'Central Africa Time', offset: 'UTC +02:00', region: 'Middle East & Africa' },
  { code: 'WAT', name: 'West Africa Time', offset: 'UTC +01:00', region: 'Middle East & Africa' },
  { code: 'SAST', name: 'South Africa Standard Time', offset: 'UTC +02:00', region: 'Middle East & Africa' },
];

export function getDesignationForOffset(offset: string): string {
  const match = TIMEZONE_DESIGNATIONS.find((tz) => tz.offset === offset);
  return match ? match.code : '';
}

export function getOffsetForDesignation(code: string): string | null {
  const match = TIMEZONE_DESIGNATIONS.find((tz) => tz.code === code);
  return match ? match.offset : null;
}
