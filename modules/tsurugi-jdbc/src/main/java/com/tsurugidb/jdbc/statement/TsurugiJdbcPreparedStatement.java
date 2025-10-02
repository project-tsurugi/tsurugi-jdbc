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
package com.tsurugidb.jdbc.statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.util.SqlCloser;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;

/**
 * Tsurugi JDBC Prepared Statement.
 */
public class TsurugiJdbcPreparedStatement extends TsurugiJdbcStatement implements PreparedStatement {

    private final String sql;
    private final TsurugiJdbcParameterGenerator parameterGenerator;

    private final List<Placeholder> lowPlaceholderList = new ArrayList<>();
    private final List<Parameter> lowParameterList = new ArrayList<>();
    private List<List<Parameter>> batchParameterList = null;

    private com.tsurugidb.tsubakuro.sql.PreparedStatement lowPreparedStatement = null;

    /**
     * Creates a new instance.
     *
     * @param factory    factory
     * @param connection connection
     * @param config     statement configuration
     * @param sql        SQL
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcPreparedStatement(TsurugiJdbcFactory factory, TsurugiJdbcConnection connection, TsurugiJdbcStatementConfig config, String sql) {
        super(factory, connection, config);
        this.sql = sql;
        this.parameterGenerator = factory.createParameterGenerator(this);
    }

    /**
     * Set convert utility.
     *
     * @param convertUtil convert utility
     */
    public void setConvertUtil(@Nonnull TsurugiJdbcConvertUtil convertUtil) {
        parameterGenerator.setConvertUtil(convertUtil);
    }

    /**
     * Get SQL type utility.
     *
     * @return SQL type utility
     */
    protected TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return getFactory().getSqlTypeUtil();
    }

    /**
     * Get low-level placeholder list.
     *
     * @return low-level placeholder list
     */
    @TsurugiJdbcInternal
    public List<Placeholder> getLowPlaceholderList() {
        return this.lowPlaceholderList;
    }

    /**
     * Get low-level prepared statement.
     *
     * @return low-level prepared statement
     * @throws SQLException if a database access error occurs
     */
    protected com.tsurugidb.tsubakuro.sql.PreparedStatement getLowPreparedStatement() throws SQLException {
        if (this.lowPreparedStatement == null) {
            var sqlClient = connection.getLowSqlClient();
            try {
                int timeout = config.getDefaultTimeout();
                var io = getIoUtil();
                this.lowPreparedStatement = io.get(sqlClient.prepare(sql, lowPlaceholderList), timeout);
            } catch (Exception e) {
                throw getExceptionHandler().sqlException("LowPreparedStatement create error", e);
            }
        }
        return this.lowPreparedStatement;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        closeExecutingResultSet();

        var lowPs = getLowPreparedStatement();

        var transaction = connection.getTransaction();
        var rs = transaction.executeOnly(lowTransaction -> {
            var future = lowTransaction.executeQuery(lowPs, lowParameterList);
            return factory.createResultSet(this, transaction, future, config);
        });

        setExecutingResultSet(rs);
        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        closeExecutingResultSet();

        var lowPs = getLowPreparedStatement();

        int timeout = config.getExecuteTimeout();

        var transaction = connection.getTransaction();
        ExecuteResult result = transaction.executeAndAutoCommit(lowTransaction -> {
            var io = getIoUtil();
            return io.get(lowTransaction.executeStatement(lowPs, lowParameterList), timeout);
        });

        long count = 0;
        for (long c : result.getCounters().values()) {
            count += c;
        }
        return (int) count;
    }

    @FunctionalInterface
    private interface ParameterGenerator {
        public Parameter generate(String name) throws SQLException;
    }

    /**
     * Set parameter.
     *
     * @param parameterIndex     parameter index (1-origin)
     * @param atomType           AtomType
     * @param parameterGenerator parameter generator
     * @throws SQLException if data convert error occurs
     */
    protected void setParameter(int parameterIndex, AtomType atomType, ParameterGenerator parameterGenerator) throws SQLException {
        String name = placeholderName(parameterIndex);

        if (this.lowPreparedStatement == null) {
            setLowPlaceholder(parameterIndex, name, atomType);
        }

        Parameter lowParameter;
        try {
            lowParameter = parameterGenerator.generate(name);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("setParameter error", e);
        }
        setLowParameter(parameterIndex, lowParameter);
    }

    /**
     * Create placeholder name.
     *
     * @param parameterIndex parameter index
     * @return placeholder name
     */
    protected String placeholderName(int parameterIndex) {
        return Integer.toString(parameterIndex);
    }

    /**
     * Set placeholder.
     *
     * @param parameterIndex parameter index (1-origin)
     * @param sqlType        SQL Type (java.sql.Types)
     * @throws SQLException if SQL type is not supported
     */
    public void setPlaceholder(int parameterIndex, int sqlType) throws SQLException {
        var util = getSqlTypeUtil();
        var atomType = util.toLowAtomType(sqlType);
        setLowPlaceholder(parameterIndex, atomType);
    }

    /**
     * Set placeholder.
     *
     * @param parameterIndex parameter index (1-origin)
     * @param atomType       AtomType
     */
    @TsurugiJdbcInternal
    public void setLowPlaceholder(int parameterIndex, AtomType atomType) {
        String name = placeholderName(parameterIndex);
        setLowPlaceholder(parameterIndex, name, atomType);
    }

    /**
     * Set placeholder.
     *
     * @param parameterIndex parameter index (1-origin)
     * @param parameterName  parameter name
     * @param atomType       AtomType
     */
    protected void setLowPlaceholder(int parameterIndex, String parameterName, AtomType atomType) {
        int index = parameterIndex - 1;
        while (index >= lowPlaceholderList.size()) {
            lowPlaceholderList.add(null);
        }

        var lowPlaceholder = lowPlaceholderList.get(index);

        if (lowPlaceholder == null || lowPlaceholder.getAtomType() != atomType) {
            lowPlaceholder = Placeholders.of(parameterName, atomType);
            lowPlaceholderList.set(index, lowPlaceholder);
        }
    }

    /**
     * Set parameter.
     *
     * @param parameterIndex parameter index (1-origin)
     * @param lowParameter   parameter
     */
    public void setLowParameter(int parameterIndex, Parameter lowParameter) {
        int index = parameterIndex - 1;
        while (index >= lowParameterList.size()) {
            lowParameterList.add(null);
        }

        lowParameterList.set(index, lowParameter);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (this.lowPreparedStatement == null) {
            var util = getSqlTypeUtil();
            var atomType = util.toLowAtomType(sqlType);
            setNull(parameterIndex, atomType);
        } else {
            String name = placeholderName(parameterIndex);
            var lowParameter = Parameters.ofNull(name);
            setLowParameter(parameterIndex, lowParameter);
        }
    }

    /**
     * Set null parameter.
     *
     * @param parameterIndex parameter index (1-origin)
     * @param atomType       AtomType
     * @throws SQLException if data convert error occurs
     */
    public void setNull(int parameterIndex, AtomType atomType) throws SQLException {
        setParameter(parameterIndex, atomType, Parameters::ofNull);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        var atomType = AtomType.BOOLEAN;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        var atomType = AtomType.INT4;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        var atomType = AtomType.INT4;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        var atomType = AtomType.INT4;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        var atomType = AtomType.INT8;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        var atomType = AtomType.FLOAT4;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        var atomType = AtomType.FLOAT8;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        var atomType = AtomType.DECIMAL;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        var atomType = AtomType.OCTET;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        var atomType = AtomType.DATE;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        var atomType = AtomType.TIME_OF_DAY;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        var atomType = AtomType.TIME_POINT;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createAsciiStream(name, x, length));
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createUnicodeStream(name, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        var atomType = AtomType.OCTET;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createBinaryStream(name, x, length));
    }

    @Override
    public void clearParameters() throws SQLException {
        this.lowParameterList.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        var util = getSqlTypeUtil();
        var atomType = util.toLowAtomType(targetSqlType);
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x, atomType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            throw getExceptionHandler().dataNullValueNoIndicatorParameterException("setObject error");
        }

        var atomType = parameterGenerator.toAtomType(x.getClass());
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x, atomType));
    }

    @Override
    public boolean execute() throws SQLException {
        closeExecutingResultSet();

        var lowPs = getLowPreparedStatement();

        var transaction = connection.getTransaction();

        if (lowPs.hasResultRecords()) {
            var rs = transaction.executeOnly(lowTransaction -> {
                var future = lowTransaction.executeQuery(lowPs, lowParameterList);
                return factory.createResultSet(this, transaction, future, config);
            });

            setExecutingResultSet(rs);
            return true;
        } else {
            int timeout = config.getExecuteTimeout();
            ExecuteResult lowResult = transaction.executeAndAutoCommit(lowTransaction -> {
                var io = getIoUtil();
                return io.get(lowTransaction.executeStatement(lowPs, lowParameterList), timeout);
            });

            setLowUpdateResult(lowResult);
            return false;
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch(sql) not supported for PreparedStatement");
    }

    @Override
    public void addBatch() throws SQLException {
        var parameter = List.copyOf(this.lowParameterList);

        if (this.batchParameterList == null) {
            this.batchParameterList = new ArrayList<>();
        }
        batchParameterList.add(parameter);
    }

    @Override
    public void clearBatch() throws SQLException {
        var parameterList = this.batchParameterList;
        if (parameterList != null) {
            parameterList.clear();
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        closeExecutingResultSet();

        var parameterList = this.batchParameterList;
        if (parameterList == null || parameterList.isEmpty()) {
            return new int[0];
        }

        var lowPs = getLowPreparedStatement();

        int timeout = config.getExecuteTimeout();

        var transaction = connection.getTransaction();
        int[] result = transaction.executeAndAutoCommit(lowTransaction -> {
            int[] count = new int[parameterList.size()];

            int i = 0;
            for (List<Parameter> parameter : parameterList) {
                var io = getIoUtil();
                var er = io.get(lowTransaction.executeStatement(lowPs, parameter), timeout);
                count[i++] = getUpdateCount(er);
            }

            return count;
        });

        clearBatch();
        return result;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createCharacterStream(name, reader, length));
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMetaData not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        var atomType = AtomType.DATE;
        var zone = cal.getTimeZone().toZoneId();
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x, zone));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        var atomType = AtomType.TIME_OF_DAY;
        var zone = cal.getTimeZone().toZoneId();
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x, zone));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        var atomType = AtomType.TIME_POINT;
        var zone = cal.getTimeZone().toZoneId();
        setParameter(parameterIndex, atomType, name -> parameterGenerator.create(name, x, zone));
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNull not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setURL not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public TsurugiJdbcParameterMetaData getParameterMetaData() throws SQLException {
        // Placeholderをセットした後でしか有効でない
        return getFactory().createParameterMetaDate(this);
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNString not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("setObject not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        int intLength = toIntLength(length);
        setAsciiStream(parameterIndex, x, intLength);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        int intLength = toIntLength(length);
        setBinaryStream(parameterIndex, x, intLength);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        int intLength = toIntLength(length);
        setCharacterStream(parameterIndex, reader, intLength);
    }

    /**
     * Convert length to int.
     *
     * @param length length
     * @return int length
     * @throws SQLException if the length overflows an int
     */
    protected int toIntLength(long length) throws SQLException {
        try {
            return Math.toIntExact(length);
        } catch (ArithmeticException e) {
            throw getExceptionHandler().dataException("Not supported length", e);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createAsciiStream(name, x, -1));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createBinaryStream(name, x, -1));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        var atomType = AtomType.CHARACTER;
        setParameter(parameterIndex, atomType, name -> parameterGenerator.createCharacterStream(name, reader, -1));
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws SQLException {
        SqlCloser superCloser = () -> {
            super.close();
        };

        try (superCloser) {
            var ps = this.lowPreparedStatement;
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("PreparedStatement close error", e);
        }
    }
}
