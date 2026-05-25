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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class LimitInputStreamTest {

    @Test
    void read1() throws IOException {
        byte[] input = { 1, 2, 3, 4 };
        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 3)) {
            assertEquals(1, target.read());
            assertEquals(2, target.read());
            assertEquals(3, target.read());
            assertEquals(-1, target.read());
        }

        try (var target = new LimitInputStream(new ByteArrayInputStream(input), input.length)) {
            assertEquals(1, target.read());
            assertEquals(2, target.read());
            assertEquals(3, target.read());
            assertEquals(4, target.read());
            assertEquals(-1, target.read());
        }

        try (var target = new LimitInputStream(new ByteArrayInputStream(input), input.length + 1)) {
            assertEquals(1, target.read());
            assertEquals(2, target.read());
            assertEquals(3, target.read());
            assertEquals(4, target.read());
            assertEquals(-1, target.read());
        }
    }

    @Test
    void readBuffer() throws IOException {
        byte[] input = { 1, 2, 3, 4, 5 };
        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 3)) {
            byte[] buf = new byte[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 1, 2 }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new byte[] { 3 }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 4)) {
            byte[] buf = new byte[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 1, 2 }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 3, 4 }, buf);
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 5)) {
            byte[] buf = new byte[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 1, 2 }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 3, 4 }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new byte[] { 5 }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }

        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 6)) {
            byte[] buf = new byte[2];
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 1, 2 }, buf);
            assertEquals(2, target.read(buf));
            assertArrayEquals(new byte[] { 3, 4 }, buf);
            assertEquals(1, target.read(buf));
            assertArrayEquals(new byte[] { 5 }, Arrays.copyOf(buf, 1));
            assertEquals(-1, target.read(buf));
        }
    }

    @Test
    void readBufferOffset() throws IOException {
        byte[] input = { 1, 2, 3, 4, 5 };
        try (var target = new LimitInputStream(new ByteArrayInputStream(input), 3)) {
            byte[] buf = new byte[4];
            assertEquals(2, target.read(buf, 1, 2));
            assertArrayEquals(new byte[] { 0, 1, 2, 0 }, buf);
            assertEquals(1, target.read(buf, 1, 2));
            assertArrayEquals(new byte[] { 0, 3, 2, 0 }, buf);
            assertEquals(-1, target.read(buf, 1, 2));
        }
    }
}
