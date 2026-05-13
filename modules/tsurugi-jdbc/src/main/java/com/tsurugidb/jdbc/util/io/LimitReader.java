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
import java.io.Reader;

/**
 * Reader that limits the number of characters that can be read.
 *
 * @since 0.5.0
 */
public class LimitReader extends Reader {

    /**
     * Creates a new instance.
     *
     * @param in    the reader to read from
     * @param limit the maximum number of characters that can be read
     * @return a new LimitReader instance, or null if the input reader is null
     */
    public static LimitReader of(Reader in, long limit) {
        if (in == null) {
            return null;
        }
        return new LimitReader(in, limit);
    }

    private final Reader in;
    private final long limit;
    private long readCount;

    /**
     * Creates a new instance.
     *
     * @param in    the reader to read from
     * @param limit the maximum number of characters that can be read
     */
    public LimitReader(Reader in, long limit) {
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
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (this.readCount >= this.limit) {
            return -1;
        }

        int minLen = (int) Math.min(len, this.limit - this.readCount);
        int readLen = in.read(cbuf, off, minLen);
        if (readLen >= 0) {
            this.readCount += readLen;
        }
        return readLen;
    }

    @Override
    public boolean ready() throws IOException {
        return in.ready();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
