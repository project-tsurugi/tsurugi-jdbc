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
package com.tsurugidb.jdbc.statement.type;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Tsurugi JDBC Clob.
 *
 * @since 0.5.0
 */
public class TsurugiJdbcClob implements Clob {

    private StringBuilder buffer;

    /**
     * Creates a new instance.
     *
     * @param capacity initial capacity
     */
    public TsurugiJdbcClob(int capacity) {
        this.buffer = new StringBuilder(capacity);
    }

    /**
     * Creates a new instance.
     *
     * @param buffer buffer
     */
    public TsurugiJdbcClob(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public long length() throws SQLException {
        checkValid();
        return buffer.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        checkValid();

        int start = (int) pos - 1;
        if (buffer.length() == 0 && start == 0) {
            return "";
        }
        int end = Math.min(buffer.length(), start + length);
        return buffer.substring(start, end);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        checkValid();
        return new StringReader(buffer.toString());
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        checkValid();

        String s = getSubString(pos, (int) length);
        return new StringReader(s);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        checkValid();

        byte[] bytes = buffer.toString().getBytes(StandardCharsets.UTF_8);
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        checkValid();

        int from = (int) start - 1;
        if (searchstr.isEmpty()) {
            if (this.buffer.length() == 0 && from == 0) {
                return 1;
            }
            if (from < this.buffer.length()) {
                return start;
            }
            return -1;
        }

        int n = buffer.indexOf(searchstr, from);
        return (n >= 0) ? (n + 1) : -1;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        checkValid();

        String s = searchstr.getSubString(1, (int) searchstr.length());
        return position(s, start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        checkValid();

        int start = (int) pos - 1;
        int end = start + str.length();
        ensureCapacity(end);

        buffer.replace(start, end, str);
        return str.length();
    }

    private void ensureCapacity(int capacity) {
        if (buffer.length() < capacity) {
            buffer.setLength(capacity);
        }
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return setString(pos, str.substring(offset, offset + len));
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return new TsurugiJdbcClobWriter(pos);
    }

    class TsurugiJdbcClobWriter extends Writer {
        private long pos;

        public TsurugiJdbcClobWriter(long pos) {
            this.pos = pos;
        }

        @Override
        public void write(int c) throws IOException {
            String s = String.valueOf((char) c);
            write(s, 0, s.length());
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            String s = new String(cbuf, off, len);
            write(s, 0, s.length());
        }

        @Override
        public void write(String str, int off, int len) throws IOException {
            try {
                setString(pos, str, off, len);
                this.pos += len;
            } catch (SQLException e) {
                throw new IOException("Failed to write to Clob", e);
            }
        }

        @Override
        public void flush() throws IOException {
            try {
                TsurugiJdbcClob.this.checkValid();
            } catch (SQLException e) {
                throw new IOException("Clob has been freed", e);
            }
        }

        @Override
        public void close() throws IOException {
            // Nothing to close
        }
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return new TsurugiJdbcClobOutputStream(pos);
    }

    class TsurugiJdbcClobOutputStream extends OutputStream {
        private long pos;

        public TsurugiJdbcClobOutputStream(long pos) {
            this.pos = pos;
        }

        @Override
        public void write(int b) throws IOException {
            String s = String.valueOf((char) (b & 0xff));
            try {
                setString(pos, s);
                this.pos++;
            } catch (SQLException e) {
                throw new IOException("Failed to write to Clob", e);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            String s = new String(b, off, len, StandardCharsets.UTF_8);
            try {
                setString(pos, s);
                this.pos += s.length();
            } catch (SQLException e) {
                throw new IOException("Failed to write to Clob", e);
            }
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        checkValid();
        try {
            buffer.setLength((int) len);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Invalid length: " + len, e);
        }
    }

    @Override
    public void free() throws SQLException {
        this.buffer = null;
    }

    private void checkValid() throws SQLException {
        if (this.buffer == null) {
            throw new SQLException("Clob has been freed");
        }
    }
}
