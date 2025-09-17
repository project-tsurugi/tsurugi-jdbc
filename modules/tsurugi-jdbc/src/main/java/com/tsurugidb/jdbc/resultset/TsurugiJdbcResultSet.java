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
package com.tsurugidb.jdbc.resultset;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.util.SqlCloser;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

@NotThreadSafe
public class TsurugiJdbcResultSet implements ResultSet, HasFactory {

    private TsurugiJdbcFactory factory;
    private final TsurugiJdbcStatement ownerStatement;
    private final TsurugiJdbcTransaction transaction;
    private final TsurugiJdbcResultSetProperties properties;
    private final TsurugiJdbcResultSetConverter resultSetConverter;
    private PreparedStatement lowPreparedStatement = null; // close on ResultSet.close()

    private FutureResponse<com.tsurugidb.tsubakuro.sql.ResultSet> resultSetFuture;
    private com.tsurugidb.tsubakuro.sql.ResultSet lowResultSet = null;

    private TsurugiJdbcResultSetGetter[] getters;
    private Object[] values;
    private Map<String, Integer> columnNameIndexMap;
    private boolean wasNull;

    private int currentRowNumber = 0;
    private boolean isAfterLast = false;
    private boolean finished = false;
    private boolean closed = false;

    public TsurugiJdbcResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<com.tsurugidb.tsubakuro.sql.ResultSet> resultSetFuture,
            TsurugiJdbcResultSetProperties properties) {
        this.ownerStatement = statement;
        this.transaction = transaction;
        this.resultSetFuture = resultSetFuture;
        this.properties = properties;

        var factory = ownerStatement.getFactory();
        this.resultSetConverter = factory.createResultSetConverter(this);
    }

    protected TsurugiJdbcResultSetConverter getConverter() {
        return this.resultSetConverter;
    }

    public void setConvertUtil(@Nonnull TsurugiJdbcConvertUtil convertUtil) {
        resultSetConverter.setConvertUtil(convertUtil);
    }

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = factory;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        var f = this.factory;
        if (f != null) {
            return f;
        }
        return ownerStatement.getFactory();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw factory.getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public void setLowPreparedStatement(PreparedStatement lowPs) {
        this.lowPreparedStatement = lowPs;
    }

    @TsurugiJdbcInternal
    public TsurugiJdbcTransaction getTransaction() {
        return this.transaction;
    }

    protected com.tsurugidb.tsubakuro.sql.ResultSet getLowResultSet() throws SQLException {
        if (this.lowResultSet == null) {
            int timeout = properties.getQueryTimeout();
            try {
                this.lowResultSet = resultSetFuture.await(timeout, TimeUnit.SECONDS);
            } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
                throw factory.getExceptionHandler().sqlException("LowResultSet get error", e);
            }
            this.resultSetFuture = null;

            lowResultSet.setTimeout(timeout, TimeUnit.SECONDS);
        }
        return this.lowResultSet;
    }

    protected ResultSetMetadata getLowResultSetMetadata(com.tsurugidb.tsubakuro.sql.ResultSet rs) throws SQLException {
        try {
            return rs.getMetadata();
        } catch (IOException | InterruptedException | ServerException e) {
            throw factory.getExceptionHandler().sqlException("ResultSet getMetadata error", e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        var lowRs = getLowResultSet();
        if (nextLowRow(lowRs)) {
            this.currentRowNumber++;

            initializeBuffer(lowRs);
            for (int i = 0; nextLowColumn(lowRs); i++) {
                var getter = getters[i];
                values[i] = fetchLowValue(lowRs, getter);
            }
            return true;
        } else {
            this.isAfterLast = true;

            if (transaction.isAutoCommit()) {
                close(); // commit
            }
            return false;
        }
    }

    private boolean nextLowRow(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        try {
            return lowRs.nextRow();
        } catch (IOException | InterruptedException | ServerException e) {
            throw factory.getExceptionHandler().sqlException("ResultSet nextRow error", e);
        }
    }

    private void initializeBuffer(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        if (this.values == null) {
            var metadata = getLowResultSetMetadata(lowRs);
            var columnList = metadata.getColumns();

            var getters = new TsurugiJdbcResultSetGetter[columnList.size()];
            for (int i = 0; i < columnList.size(); i++) {
                var column = columnList.get(i);
                getters[i] = TsurugiJdbcResultSetGetter.of(column);
            }
            this.getters = getters;

            this.values = new Object[columnList.size()];
        }
    }

    private boolean nextLowColumn(com.tsurugidb.tsubakuro.sql.ResultSet lowRs) throws SQLException {
        try {
            return lowRs.nextColumn();
        } catch (IOException | InterruptedException | ServerException e) {
            throw factory.getExceptionHandler().sqlException("ResultSet nextColumn error", e);
        }
    }

    private Object fetchLowValue(com.tsurugidb.tsubakuro.sql.ResultSet lowRs, TsurugiJdbcResultSetGetter getter) throws SQLException {
        try {
            return getter.fetchValue(this, lowRs);
        } catch (IOException | InterruptedException | ServerException e) {
            throw factory.getExceptionHandler().sqlException("ResultSet fetchValue error", e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToString(value);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToBoolean(value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToByte(value);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToShort(value);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToInt(value);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToLong(value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToFloat(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToDouble(value);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToDecimal(value, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToBytes(value);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToDate(value);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToTime(value);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToTimestamp(value);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToAsciiStream(value);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToUnicodeStream(value);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToBinaryStream(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBytes(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBinaryStream(columnIndex);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    @TsurugiJdbcNotSupported
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object value;
        try {
            value = values[columnIndex - 1];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
        }
        this.wasNull = (value == null);
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int index = findColumn(columnLabel);
        return getObject(index);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (this.columnNameIndexMap == null) {
            var rs = getLowResultSet();
            var metadata = getLowResultSetMetadata(rs);
            var columnList = metadata.getColumns();

            var map = new HashMap<String, Integer>(columnList.size());
            for (int i = 0; i < columnList.size(); i++) {
                var column = columnList.get(i);
                String name = column.getName();
                if (name != null) {
                    map.put(name, i + 1);
                }
            }
            this.columnNameIndexMap = map;
        }

        Integer index = columnNameIndexMap.get(columnLabel);
        if (index == null) {
            throw new SQLException(); // TODO TsurugiSQLExceptionHandler
        }
        return index;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToCharacterStream(value);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getCharacterStream(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToDecimal(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.currentRowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.isAfterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.currentRowNumber == 1;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("beforeFirst not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("afterLast not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("first not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("last not supported");
    }

    @Override
    public int getRow() throws SQLException {
        return this.currentRowNumber;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("absolute not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean relative(int rows) throws SQLException {
        switch (rows) {
        case 0:
            return true;
        case 1:
            return next();
        default:
            throw new SQLFeatureNotSupportedException("relative not supported");
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("previous not supported");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Unsupported direction");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setFetchSize(int rows) throws SQLException {
        // not supported
    }

    @Override
    @TsurugiJdbcNotSupported
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowUpdated not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowInserted not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("rowDeleted not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("insertRow not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRow not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("deleteRow not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToInsertRow not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow not supported");
    }

    @Override
    public TsurugiJdbcStatement getStatement() throws SQLException {
        return this.ownerStatement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @TsurugiJdbcNotSupported
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToBlob(value);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToClob(value);
    }

    @Override
    @TsurugiJdbcNotSupported
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @TsurugiJdbcNotSupported
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBlob(columnIndex);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getClob(columnIndex);
    }

    @Override
    @TsurugiJdbcNotSupported
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // FIXME calendar
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToDate(value);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // FIXME calendar
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToTime(value);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // FIXME calendar
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToTimestamp(value);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex, cal);
    }

    @Override
    @TsurugiJdbcNotSupported
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getObject(columnIndex);
        var converter = getConverter();
        return converter.convertToType(value, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex, type);
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;

        SqlCloser statement = () -> {
            if (ownerStatement.isCloseOnCompletion()) {
                ownerStatement.close();
            }
        };
        SqlCloser commit = this::finish; // commit when AutoCommit

        // The lowResultSet must be closed before commit.
        try (statement; var ps = lowPreparedStatement; commit; var f = resultSetFuture; var rs = lowResultSet) {
            // close only
        } catch (IOException | InterruptedException | ServerException e) {
            throw factory.getExceptionHandler().sqlException("ResultSet close error", e);
        }
    }

    protected void finish() throws SQLException {
        if (!finished) {
            this.finished = true;

            if (transaction.isAutoCommit()) {
                transaction.commit();
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }
}
