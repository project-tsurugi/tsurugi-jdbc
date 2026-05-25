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

import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream that limits the number of bytes that can be read.
 *
 * @since 0.5.0
 */
public class LimitInputStream extends InputStream {

    /**
     * Creates a new instance.
     *
     * @param in    the input stream to read from
     * @param limit the maximum number of bytes that can be read from the input stream (must be non-negative)
     * @return a new LimitInputStream instance, or null if the input stream is null
     */
    public static LimitInputStream of(InputStream in, long limit) {
        if (in == null) {
            return null;
        }
        return new LimitInputStream(in, limit);
    }

    private final InputStream in;
    private final long limit;
    private long readCount;

    /**
     * Creates a new instance.
     *
     * @param in    the input stream to read from
     * @param limit the maximum number of bytes that can be read from the input stream (must be non-negative)
     */
    public LimitInputStream(InputStream in, long limit) {
        this.in = in;
        this.limit = limit;
        this.readCount = 0;
    }

    @Override
    public int read() throws IOException {
        if (this.readCount >= this.limit) {
            return -1;
        }

        int result = in.read();
        if (result != -1) {
            this.readCount++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        if (this.readCount >= this.limit) {
            return -1;
        }

        int minLen = (int) Math.min(len, this.limit - this.readCount);
        int readLen = in.read(b, off, minLen);
        if (readLen >= 0) {
            this.readCount += readLen;
        }
        return readLen;
    }

    @Override
    public int available() throws IOException {
        long remaining = this.limit - this.readCount;

        if (remaining <= 0) {
            return 0;
        }

        return (int) Math.min(in.available(), remaining);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
