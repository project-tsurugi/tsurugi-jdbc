/*
 * Copyright 2025 Project Tsurugi.
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
package com.tsurugidb.jdbc.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

public class ClobTestMock implements java.sql.Clob {

    private final String value;

    public ClobTestMock(String value) {
        this.value = value;
    }

    @Override
    public long length() throws SQLException {
        return value.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return value.substring((int) pos, (int) pos + length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(value);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void free() throws SQLException {
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
