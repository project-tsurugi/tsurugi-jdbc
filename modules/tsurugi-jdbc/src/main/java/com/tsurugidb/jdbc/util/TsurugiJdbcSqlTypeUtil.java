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

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.util.type.TsurugiJdbcType;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * Tsurugi JDBC SQL Type Utility.
 */
@TsurugiJdbcInternal
public class TsurugiJdbcSqlTypeUtil {

    /**
     * Convert from java.sql.Types to AtomType.
     *
     * @param sqlType SQL type (java.sql.Types)
     * @return AtomType
     * @throws SQLException if SQL type is not supported
     */
    public AtomType toLowAtomType(int sqlType) throws SQLException {
        switch (sqlType) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            return AtomType.BOOLEAN;
        case java.sql.Types.TINYINT:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.INTEGER:
            return AtomType.INT4;
        case java.sql.Types.BIGINT:
            return AtomType.INT8;
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            return AtomType.FLOAT4;
        case java.sql.Types.DOUBLE:
            return AtomType.FLOAT8;
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
            return AtomType.DECIMAL;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
            return AtomType.CHARACTER;
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
        case java.sql.Types.LONGVARBINARY:
            return AtomType.OCTET;
        case java.sql.Types.DATE:
            return AtomType.DATE;
        case java.sql.Types.TIME:
            return AtomType.TIME_OF_DAY;
        case java.sql.Types.TIMESTAMP:
            return AtomType.TIME_POINT;
        case java.sql.Types.TIME_WITH_TIMEZONE:
            return AtomType.TIME_OF_DAY_WITH_TIME_ZONE;
        case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
            return AtomType.TIME_POINT_WITH_TIME_ZONE;
        case java.sql.Types.BLOB:
            return AtomType.BLOB;
        case java.sql.Types.CLOB:
            return AtomType.CLOB;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported sqlType {0}", sqlType));
        }
    }

    /**
     * Get signed.
     *
     * @param type type
     * @return signed
     * @throws SQLException if type is not supported
     */
    public boolean getSigned(TsurugiJdbcType type) throws SQLException {
        var jdbcType = type.getJdbcType();
        switch (jdbcType) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case REAL:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case NUMERIC:
            return true;
        default:
            return false;
        }
    }

    /**
     * Get display size.
     *
     * @param type type
     * @return display size
     * @throws SQLException if type is not supported
     */
    public int getDisplaySize(TsurugiJdbcType type) throws SQLException {
        var jdbcType = type.getJdbcType();
        switch (jdbcType) {
        case BOOLEAN:
            return 1;
        case TINYINT:
            return 4;
        case SMALLINT:
            return 6;
        case INTEGER:
            return 11;
        case BIGINT:
            return 20;
        case REAL:
        case FLOAT:
            return 15;
        case DOUBLE:
            return 25;
        case DECIMAL:
        case NUMERIC:
            return getPrecision(type);
        case CHAR:
        case VARCHAR:
        case LONGNVARCHAR:
            return getLength(type);
        case BINARY:
        case VARBINARY:
        case LONGVARBINARY:
            return getLength(type) * 2;
        case DATE:
            return 10; // yyyy-MM-dd
        case TIME:
            return 18; // HH:mm:ss.SSSSSSSSS
        case TIMESTAMP:
            return 10 + 1 + 18; // yyyy-MM-dd HH:mm:ss.SSSSSSSSS
        case TIME_WITH_TIMEZONE:
            return 18 + 6; // HH:mm:ss.SSSSSSSSS+09:00
        case TIMESTAMP_WITH_TIMEZONE:
            return 10 + 1 + 18 + 6; // yyyy-MM-dd HH:mm:ss.SSSSSSSSS+09:00
        case BLOB:
            return Integer.MAX_VALUE;
        case CLOB:
            return Integer.MAX_VALUE;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported JDBCType.{0}", jdbcType));
        }
    }

    /**
     * Get column size.
     *
     * @param type type
     * @return column size
     * @throws SQLException if atom type is not supported
     */
    public Integer getColumnSize(TsurugiJdbcType type) throws SQLException {
        var atomType = type.getAtomType();
        switch (atomType) {
        case BOOLEAN:
            return 1;
        case INT4:
            return 32;
        case INT8:
            return 64;
        case FLOAT4:
            return 38;
        case FLOAT8:
            return 308;
        case DECIMAL:
            return null;
        case CHARACTER:
        case OCTET:
            return getLength(type);
        default:
            return null;
        }
    }

    /**
     * Get length.
     *
     * @param type type
     * @return length
     */
    public int getLength(TsurugiJdbcType type) {
        var lengthOpt = type.findLength();
        if (lengthOpt.isPresent()) {
            var value = lengthOpt.get();
            if (value.arbitrary()) {
                return 2097132;
            } else {
                return value.value();
            }
        }
        return 0;
    }

    /**
     * Get octet length.
     *
     * @param type type
     * @return octet length
     * @throws SQLException if type is not supported
     */
    public Integer getOctetLength(TsurugiJdbcType type) throws SQLException {
        var atomType = type.getAtomType();
        switch (atomType) {
        case CHARACTER:
        case OCTET:
            var lengthOpt = type.findLength();
            if (lengthOpt.isPresent()) {
                var value = lengthOpt.get();
                if (value.arbitrary()) {
                    return 2097132;
                } else {
                    return value.value();
                }
            }
            return 0;
        default:
            return null;
        }
    }

    /**
     * Get numeric precision radix.
     *
     * @param type type
     * @return numeric precision radix
     * @throws SQLException if type is not supported
     */
    public Integer getNumPrecRadix(TsurugiJdbcType type) throws SQLException {
        var atomType = type.getAtomType();
        switch (atomType) {
        case BOOLEAN:
        case INT4:
        case INT8:
            return 2;
        case FLOAT4:
        case FLOAT8:
        case DECIMAL:
            return 10;
        default:
            return null;
        }
    }

    /**
     * Get decimal digits.
     *
     * @param type type
     * @return decimal digits
     * @throws SQLException if type is not supported
     */
    public Integer getDecimalDigits(TsurugiJdbcType type) throws SQLException {
        var atomType = type.getAtomType();
        switch (atomType) {
        case DECIMAL:
            return getScale(type);
        case TIME_OF_DAY:
        case TIME_POINT:
        case TIME_OF_DAY_WITH_TIME_ZONE:
        case TIME_POINT_WITH_TIME_ZONE:
            return 9;
        default:
            return null;
        }
    }

    /**
     * Get precision.
     *
     * @param type type
     * @return precision
     */
    public int getPrecision(TsurugiJdbcType type) {
        var precisionOpt = type.findPrecision();
        if (precisionOpt.isPresent()) {
            var value = precisionOpt.get();
            if (value.arbitrary()) {
                return 38;
            } else {
                return value.value();
            }
        }
        return 0;
    }

    /**
     * Get scale.
     *
     * @param type type
     * @return scale
     */
    public int getScale(TsurugiJdbcType type) {
        var scaleOpt = type.findScale();
        if (scaleOpt.isPresent()) {
            var value = scaleOpt.get();
            if (value.arbitrary()) {
                return 0;
            } else {
                return value.value();
            }
        }
        return 0;
    }

    /**
     * Type info values list.
     */
    public static final List<Object[]> TYPE_INFO_VALUES_LIST;
    static {
        var list = new ArrayList<TsurugiJdbcTypeInfo>();
        list.add(TsurugiJdbcTypeInfo.of("BOOLEAN", JDBCType.BOOLEAN));

        list.add(TsurugiJdbcTypeInfo.of("INT", JDBCType.INTEGER).unsignedAttribute(false));
        list.add(TsurugiJdbcTypeInfo.of("BIGINT", JDBCType.BIGINT).unsignedAttribute(false).autoIncrement(true));

        var f = TsurugiJdbcTypeInfo.of("REAL", JDBCType.REAL).unsignedAttribute(false);
        list.add(f);
        list.add(f.clone("FLOAT", JDBCType.FLOAT));
        list.add(TsurugiJdbcTypeInfo.of("DOUBLE", JDBCType.DOUBLE).unsignedAttribute(false));

        var d = TsurugiJdbcTypeInfo.of("DECIMAL", JDBCType.DECIMAL).unsignedAttribute(false);
        list.add(d);
        list.add(d.clone("NUMERIC", JDBCType.NUMERIC));

        var c = TsurugiJdbcTypeInfo.of("CHAR", JDBCType.CHAR).literalPrefix("'", "'").searchable(DatabaseMetaData.typeSearchable);
        list.add(c);
        list.add(c.clone("VARCHAR", JDBCType.VARCHAR));

        var b = TsurugiJdbcTypeInfo.of("BINARY", JDBCType.BINARY).literalPrefix("X'", "'");
        list.add(b);
        list.add(b.clone("VARBINARY", JDBCType.VARBINARY));

        list.add(TsurugiJdbcTypeInfo.of("DATE", JDBCType.DATE).literalPrefix("DATE'", "'"));
        list.add(TsurugiJdbcTypeInfo.of("TIME", JDBCType.TIME).literalPrefix("TIME'", "'"));
        list.add(TsurugiJdbcTypeInfo.of("TIMESTAMP", JDBCType.TIMESTAMP).literalPrefix("TIMESTAMP'", "'"));
        list.add(TsurugiJdbcTypeInfo.of("TIME WITH TIME ZONE", JDBCType.TIME_WITH_TIMEZONE).literalPrefix("TIME WITH TIME ZONE'", "'"));
        list.add(TsurugiJdbcTypeInfo.of("TIMESTAMP WITH TIME ZONE", JDBCType.TIMESTAMP_WITH_TIMEZONE).literalPrefix("TIMESTAMP WITH TIME ZONE'", "'"));

        list.add(TsurugiJdbcTypeInfo.of("BLOB", JDBCType.BLOB).literalPrefix("X'", "'"));
        list.add(TsurugiJdbcTypeInfo.of("CLOB", JDBCType.CLOB).literalPrefix("'", "'"));

        TYPE_INFO_VALUES_LIST = list.stream() //
                .sorted(Comparator.comparing(TsurugiJdbcTypeInfo::getSqlType)) //
                .map(TsurugiJdbcTypeInfo::toValues) //
                .collect(Collectors.toList());
    }
}
