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

package com.example.spanner.quicksink.util;

public class Log {
    public static final int LEVEL_INFO = 1;
    public static final int LEVEL_DEBUG = 2;
    public static final int LEVEL_TRACE = 3;

    private static int currentLevel = LEVEL_INFO;

    public static void setLevel(int level) {
        currentLevel = level;
    }

    public static boolean isEnabled(int level) {
        return currentLevel >= level;
    }

    public static void info(String msg) {
        if (currentLevel >= LEVEL_INFO) {
            System.out.println(msg);
        }
    }
    
    public static void error(String msg) {
        System.err.println(msg);
    }

    public static void debug(String msg) {
        if (currentLevel >= LEVEL_DEBUG) {
            System.out.println(msg);
        }
    }

    public static void trace(String msg) {
        if (currentLevel >= LEVEL_TRACE) {
            System.out.println(msg);
        }
    }
}
