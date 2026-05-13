/*
 * Copyright 2025-2026 Project Tsurugi.
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
package com.tsurugidb.jdbc.util.io;

import static org.junit.jupiter.api.Assertions.*;

import java.io.StringReader;
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class LimitReaderTest {

    @Test
    void read1() throws IOException {
        String input = "abcd";
        try (var target = new LimitReader(new StringReader(input), 3)) {
            assertEquals('a', target.read());
            assertEquals('b', target.read());
            assertEquals('c', target.read());
            assertEquals(-1, target.read());
        }

        try (var target = new LimitReader(new StringReader(input), input.length())) {
            assertEquals('a', target.read());
            assertEquals('b', target.read());
            assertEquals('c', target.read());
            assertEquals('d', target.read());
            assertEquals(-1, target.read());
        }

        try (var target = new LimitReader(new StringReader(input), input.length() + 1)) {
            assertEquals('a', target.read());
            assertEquals('b', target.read());
            assertEquals('c', target.read());
            assertEquals('d', target.read());
            assertEquals(-1, target.read());
        }
    }

    @Test
    void readBuffer() throws IOException {
        String input = "abcde";
        try (var target = new LimitReader(new StringReader(input), 3)) {
            char[] buf = new char[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'a', 'b' }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new char[] { 'c' }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitReader(new StringReader(input), 4)) {
            char[] buf = new char[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'a', 'b' }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'c', 'd' }, buf);
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitReader(new StringReader(input), 5)) {
            char[] buf = new char[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'a', 'b' }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'c', 'd' }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new char[] { 'e' }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitReader(new StringReader(input), 6)) {
            char[] buf = new char[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'a', 'b' }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new char[] { 'c', 'd' }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new char[] { 'e' }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }
    }

    @Test
    void readBufferOffset() throws IOException {
        String input = "abcde";
        try (var target = new LimitReader(new StringReader(input), 3)) {
            char[] buf = new char[4];
            assertEquals(2, target.read(buf, 1, 2));
            assertArrayEquals(new char[] { '\0', 'a', 'b', '\0' }, buf);
            assertEquals(1, target.read(buf, 1, 2));
            assertArrayEquals(new char[] { '\0', 'c', 'b', '\0' }, buf);
            assertEquals(-1, target.read(buf, 1, 2));
        }
    }
}
