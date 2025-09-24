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

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

@TsurugiJdbcInternal
public class TsurugiJdbcSqlTypeUtil {

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

    public JDBCType toJdbcType(SqlCommon.Column lowColumn) throws SQLException {
        var atomType = lowColumn.getAtomType();
        switch (atomType) {
        case CHARACTER:
            if (isVarying(lowColumn)) {
                return JDBCType.VARCHAR;
            } else {
                return JDBCType.CHAR;
            }
        case OCTET:
            if (isVarying(lowColumn)) {
                return JDBCType.VARBINARY;
            } else {
                return JDBCType.BINARY;
            }
        default:
            return toJdbcType(atomType);
        }
    }

    public JDBCType toJdbcType(AtomType atomType) throws SQLException {
        switch (atomType) {
        case BOOLEAN:
            return JDBCType.BOOLEAN;
        case INT4:
            return JDBCType.INTEGER;
        case INT8:
            return JDBCType.BIGINT;
        case FLOAT4:
            return JDBCType.REAL;
        case FLOAT8:
            return JDBCType.DOUBLE;
        case DECIMAL:
            return JDBCType.DECIMAL;
        case CHARACTER:
            return JDBCType.VARCHAR;
        case OCTET:
            return JDBCType.VARBINARY;
        case DATE:
            return JDBCType.DATE;
        case TIME_OF_DAY:
            return JDBCType.TIME;
        case TIME_POINT:
            return JDBCType.TIMESTAMP;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return JDBCType.TIME_WITH_TIMEZONE;
        case TIME_POINT_WITH_TIME_ZONE:
            return JDBCType.TIMESTAMP_WITH_TIMEZONE;
        case BLOB:
            return JDBCType.BLOB;
        case CLOB:
            return JDBCType.CLOB;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }

    public String toSqlTypeName(SqlCommon.Column lowColumn) throws SQLException {
        var atomType = lowColumn.getAtomType();
        switch (atomType) {
        case CHARACTER:
            if (isVarying(lowColumn)) {
                return "VARCHAR";
            } else {
                return "CHAR";
            }
        case OCTET:
            if (isVarying(lowColumn)) {
                return "VARBINARY";
            } else {
                return "BINARY";
            }
        default:
            return toSqlTypeName(atomType);
        }
    }

    public String toSqlTypeName(AtomType atomType) throws SQLException {
        switch (atomType) {
        case BOOLEAN:
            return "BOOLEAN";
        case INT4:
            return "INT";
        case INT8:
            return "BIGINT";
        case FLOAT4:
            return "REAL";
        case FLOAT8:
            return "DOUBLE";
        case DECIMAL:
            return "DECIMAL";
        case CHARACTER:
            return "VARCHAR";
        case OCTET:
            return "VARBINARY";
        case DATE:
            return "DATE";
        case TIME_OF_DAY:
            return "TIME";
        case TIME_POINT:
            return "TIMESTAMP";
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return "TIME WITH TIME ZONE";
        case TIME_POINT_WITH_TIME_ZONE:
            return "TIMESTAMP WITH TIME ZONE";
        case BLOB:
            return "BLOB";
        case CLOB:
            return "CLOB";
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }

    public int toDisplaySize(SqlCommon.Column lowColumn) throws SQLException {
        var atomType = lowColumn.getAtomType();
        switch (atomType) {
        case BOOLEAN:
            return 1;
        case INT4:
            return 11;
        case INT8:
            return 20;
        case FLOAT4:
            return 15;
        case FLOAT8:
            return 25;
        case DECIMAL:
            return getPrecision(lowColumn);
        case CHARACTER:
            return getLength(lowColumn);
        case OCTET:
            return getLength(lowColumn) * 2;
        case DATE:
            return 10; // yyyy-MM-dd
        case TIME_OF_DAY:
            return 18; // HH:mm:ss.SSSSSSSSS
        case TIME_POINT:
            return 10 + 1 + 18; // yyyy-MM-dd HH:mm:ss.SSSSSSSSS
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return 18 + 6; // HH:mm:ss.SSSSSSSSS+09:00
        case TIME_POINT_WITH_TIME_ZONE:
            return 10 + 1 + 18 + 6; // yyyy-MM-dd HH:mm:ss.SSSSSSSSS+09:00
        case BLOB:
            return Integer.MAX_VALUE;
        case CLOB:
            return Integer.MAX_VALUE;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }

    public Class<?> toJavaClass(SqlCommon.Column lowColumn) throws SQLException {
        var atomType = lowColumn.getAtomType();
        return toJavaClass(atomType);
    }

    public Class<?> toJavaClass(AtomType atomType) throws SQLException {
        switch (atomType) {
        case BOOLEAN:
            return boolean.class;
        case INT4:
            return int.class;
        case INT8:
            return long.class;
        case FLOAT4:
            return float.class;
        case FLOAT8:
            return double.class;
        case DECIMAL:
            return BigDecimal.class;
        case CHARACTER:
            return String.class;
        case OCTET:
            return byte[].class;
        case DATE:
            return LocalDate.class;
        case TIME_OF_DAY:
            return LocalTime.class;
        case TIME_POINT:
            return LocalDateTime.class;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return OffsetTime.class;
        case TIME_POINT_WITH_TIME_ZONE:
            return OffsetDateTime.class;
        case BLOB:
            return java.sql.Blob.class;
        case CLOB:
            return java.sql.Clob.class;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }

    public int getLength(SqlCommon.Column lowColumn) {
        var lengthOpt = findLength(lowColumn);
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

    public int getPrecision(SqlCommon.Column lowColumn) {
        var precisionOpt = findPrecision(lowColumn);
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

    public int getScale(SqlCommon.Column lowColumn) {
        var scaleOpt = findScale(lowColumn);
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

    public boolean isNullable(SqlCommon.Column lowColumn) {
        var nullableOpt = findNullable(lowColumn);
        return nullableOpt.orElse(true);
    }

    public boolean isVarying(SqlCommon.Column lowColumn) {
        var varyingOpt = findVarying(lowColumn);
        return varyingOpt.orElse(true);
    }

    /**
     * Get length for data types.
     *
     * @param lowColumn column
     * @return length
     */
    public Optional<ArbitraryInt> findLength(SqlCommon.Column lowColumn) {
        var c = lowColumn.getLengthOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case LENGTH:
            return Optional.of(ArbitraryInt.of(lowColumn.getLength()));
        case ARBITRARY_LENGTH:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case LENGTHOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Get precision for decimal types.
     *
     * @param lowColumn column
     * @return precision
     */
    public Optional<ArbitraryInt> findPrecision(SqlCommon.Column lowColumn) {
        var c = lowColumn.getPrecisionOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case PRECISION:
            return Optional.of(ArbitraryInt.of(lowColumn.getPrecision()));
        case ARBITRARY_PRECISION:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case PRECISIONOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Get scale for decimal types.
     *
     * @param lowColumn column
     * @return scale
     */
    public Optional<ArbitraryInt> findScale(SqlCommon.Column lowColumn) {
        var c = lowColumn.getScaleOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case SCALE:
            return Optional.of(ArbitraryInt.of(lowColumn.getScale()));
        case ARBITRARY_SCALE:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case SCALEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Whether the column type is nullable.
     *
     * @param lowColumn column
     * @return nullable
     */
    public Optional<Boolean> findNullable(SqlCommon.Column lowColumn) {
        var c = lowColumn.getNullableOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case NULLABLE:
            return Optional.of(lowColumn.getNullable());
        case NULLABLEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Whether the column type is varying.
     *
     * @param lowColumn column
     * @return varying
     */
    public Optional<Boolean> findVarying(SqlCommon.Column lowColumn) {
        var c = lowColumn.getVaryingOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case VARYING:
            return Optional.of(lowColumn.getVarying());
        case VARYINGOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

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
