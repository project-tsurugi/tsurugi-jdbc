package com.tsurugidb.jdbc.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSetMetaData;
import com.tsurugidb.jdbc.statement.TsurugiJdbcParameterMetaData;

public class ExpectedColumn {
    private final int ordinalPosition;
    private final String columnName;

    private String tableName = "test";
    private JDBCType dataType;
    private String typeName;
    private String typeBaseName;
    private Integer columnSize;
    private Integer bufferLength;
    private Integer decimalDigits;
    private Integer numPrecRadix;
    private boolean nullable = true;
    private String remarks = "";
    private Integer datetimeSub;
    private long displaySize;

    private boolean signed = false;
    private Integer precision;
    private Integer scale;
    private Class<?> typeClass;

    public ExpectedColumn(int ordinalPosition, String columnName) {
        this.ordinalPosition = ordinalPosition;
        this.columnName = columnName;
    }

    public ExpectedColumn initialize(String typeName) {
        this.typeName = typeName.toUpperCase();

        String baseName;
        String arg1 = null;
        Integer size1 = null;
        String arg2 = null;
        Integer size2 = null;
        {
            int n = typeName.indexOf('(');
            if (n >= 0) {
                baseName = typeName.substring(0, n).trim().toUpperCase();
                int c = typeName.indexOf(',');
                if (c >= 0) {
                    arg1 = typeName.substring(n + 1, c).trim();
                    size1 = Integer.parseInt(arg1);
                    int m = typeName.indexOf(')');
                    arg2 = typeName.substring(c + 1, m).trim();
                    size2 = Integer.parseInt(arg2);
                } else {
                    int m = typeName.indexOf(')');
                    arg1 = typeName.substring(n + 1, m).trim();
                    try {
                        size1 = Integer.parseInt(arg1);
                    } catch (NumberFormatException ignore) {
                    }
                }
            } else {
                baseName = typeName.trim().toUpperCase();
            }
        }
        this.typeBaseName = baseName;

        switch (baseName) {
        case "BOOLEAN":
            this.dataType = JDBCType.BOOLEAN;
            this.columnSize = 1;
            this.numPrecRadix = 2;
            this.bufferLength = 1;
            this.displaySize = 1;
            this.typeClass = boolean.class;
            break;
        case "INT":
            this.dataType = JDBCType.INTEGER;
            this.columnSize = 32;
            this.numPrecRadix = 2;
            this.bufferLength = 4;
            this.displaySize = 11;
            this.signed = true;
            this.typeClass = int.class;
            break;
        case "BIGINT":
            this.dataType = JDBCType.BIGINT;
            this.columnSize = 64;
            this.numPrecRadix = 2;
            this.bufferLength = 8;
            this.displaySize = 20;
            this.signed = true;
            this.typeClass = long.class;
            break;
        case "REAL":
            this.dataType = JDBCType.REAL;
            this.columnSize = 38;
            this.numPrecRadix = 10;
            this.bufferLength = 4;
            this.displaySize = 15;
            this.signed = true;
            this.typeClass = float.class;
            break;
        case "DOUBLE":
            this.dataType = JDBCType.DOUBLE;
            this.columnSize = 308;
            this.numPrecRadix = 10;
            this.bufferLength = 8;
            this.displaySize = 15;
            this.signed = true;
            this.typeClass = double.class;
            break;
        case "DECIMAL":
            this.dataType = JDBCType.DECIMAL;
            this.bufferLength = 19;
            this.decimalDigits = size2;
            this.numPrecRadix = 10;
            this.displaySize = size1;
            this.signed = true;
            this.precision = size1;
            this.scale = size2;
            this.typeClass = BigDecimal.class;
            break;
        case "CHAR":
            this.dataType = JDBCType.CHAR;
            this.columnSize = size1;
            this.bufferLength = size1;
            this.displaySize = size1;
            this.typeClass = String.class;
            break;
        case "VARCHAR":
            this.dataType = JDBCType.VARCHAR;
            this.columnSize = size1;
            this.bufferLength = size1;
            this.displaySize = size1;
            this.typeClass = String.class;
            break;
        case "BINARY":
            this.dataType = JDBCType.BINARY;
            this.columnSize = size1;
            this.bufferLength = size1;
            this.displaySize = size1;
            this.typeClass = byte[].class;
            break;
        case "VARBINARY":
            this.dataType = JDBCType.VARBINARY;
            if ("*".equals(arg1)) {
                this.columnSize = 2097132;
                this.bufferLength = 2097132;
            } else {
                this.columnSize = size1;
                this.bufferLength = size1;
            }
            this.displaySize = 2097132;
            this.typeClass = byte[].class;
            break;
        case "DATE":
            this.dataType = JDBCType.DATE;
            this.columnSize = 10;
            this.displaySize = this.columnSize;
            this.typeClass = LocalDate.class;
            break;
        case "TIME":
            this.dataType = JDBCType.TIME;
            this.columnSize = 18;
            this.decimalDigits = 9;
            this.displaySize = this.columnSize;
            this.typeClass = LocalTime.class;
            break;
        case "TIMESTAMP":
            this.dataType = JDBCType.TIMESTAMP;
            this.columnSize = 10 + 1 + 18;
            this.decimalDigits = 9;
            this.displaySize = this.columnSize;
            this.typeClass = LocalDateTime.class;
            break;
        case "TIME WITH TIME ZONE":
            this.dataType = JDBCType.TIME_WITH_TIMEZONE;
            this.columnSize = 18 + 6;
            this.decimalDigits = 9;
            this.displaySize = this.columnSize;
            this.typeClass = OffsetTime.class;
            break;
        case "TIMESTAMP WITH TIME ZONE":
            this.dataType = JDBCType.TIMESTAMP_WITH_TIMEZONE;
            this.columnSize = (10 + 1 + 18) + 6;
            this.decimalDigits = 9;
            this.displaySize = this.columnSize;
            this.typeClass = OffsetDateTime.class;
            break;
        default:
            throw new AssertionError("not yet implements. type=" + typeName);
        }

        return this;
    }

    public ExpectedColumn tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ExpectedColumn dataType(JDBCType dataType) {
        this.dataType = dataType;
        return this;
    }

    public ExpectedColumn typeName(String typeName) {
        this.typeName = typeName;
        return this;
    }

    public ExpectedColumn columnSize(Integer columnSize) {
        this.columnSize = columnSize;
        return this;
    }

    public ExpectedColumn bufferLength(Integer bufferLength) {
        this.bufferLength = bufferLength;
        return this;
    }

    public ExpectedColumn decimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
        return this;
    }

    public ExpectedColumn numPrecRadix(Integer numPrecRadix) {
        this.numPrecRadix = numPrecRadix;
        return this;
    }

    public ExpectedColumn numPrecRadix(int numPrecRadix) {
        return numPrecRadix((short) numPrecRadix);
    }

    public ExpectedColumn nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public ExpectedColumn notNull() {
        return nullable(false);
    }

    public ExpectedColumn remarks(String remarks) {
        this.remarks = remarks;
        return this;
    }

    public void test(ResultSet rs) throws SQLException {
        String databaseName = rs.getString("TABLE_CAT");
        assertNull(databaseName);

        String schemaName = rs.getString("TABLE_SCHEM");
        assertNull(schemaName);

        String tableName = rs.getString("TABLE_NAME");
        assertEquals(this.tableName, tableName);

        String columnName = rs.getString("COLUMN_NAME");
        assertEquals(this.columnName, columnName);

        var dataType = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
        assertEquals(this.dataType, dataType);

        String typeName = rs.getString("TYPE_NAME");
        assertEquals(this.typeName, typeName);

        int columnSize = rs.getInt("COLUMN_SIZE");
        assertEquals(this.columnSize, columnSize);

        int bufferLength = rs.getInt("BUFFER_LENGTH");
        assertEquals(this.bufferLength, bufferLength);

        int decimalDigits = rs.getInt("DECIMAL_DIGITS");
        if (this.decimalDigits == null) {
            assertTrue(rs.wasNull());
        } else {
            assertEquals(this.decimalDigits, decimalDigits);
        }

        int numPrecRadix = rs.getInt("NUM_PREC_RADIX");
        assertEquals(this.numPrecRadix, numPrecRadix);

        int nullable = rs.getInt("NULLABLE");
        if (this.nullable) {
            assertEquals(DatabaseMetaData.columnNullable, nullable);
        } else {
            assertEquals(DatabaseMetaData.columnNoNulls, nullable);
        }

        String remarks = rs.getString("REMARKS");
        assertEquals(this.remarks, remarks);

        String columnDefault = rs.getString("COLUMN_DEF");
        assertNull(columnDefault);

//      int sqlDataType = rs.getInt("SQL_DATA_TYPE");
//      assertEquals(this.dataType, sqlDataType);

        int sqlDatetimeSub = rs.getInt("SQL_DATETIME_SUB");
        if (this.datetimeSub == null) {
            assertTrue(rs.wasNull());
        } else {
            assertEquals(this.datetimeSub, sqlDatetimeSub);
        }

        int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
        switch (this.dataType) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
            assertEquals(this.columnSize, charOctetLength);
            break;
        default:
            assertTrue(rs.wasNull());
            break;
        }

        int ordinalPosition = rs.getInt("ORDINAL_POSITION");
        assertEquals(this.ordinalPosition, ordinalPosition);

        String isNullable = rs.getString("IS_NULLABLE");
        if (this.nullable) {
            assertEquals("YES", isNullable);
        } else {
            assertEquals("NO", isNullable);
        }
    }

    public void test(TsurugiJdbcResultSetMetaData metaData, int columnIndex) throws SQLException {
        int nullable = metaData.isNullable(columnIndex);
        assertEquals(ResultSetMetaData.columnNullableUnknown, nullable);

        boolean signed = metaData.isSigned(columnIndex);
        assertEquals(this.signed, signed);

        int displaySize = metaData.getColumnDisplaySize(columnIndex);
        assertEquals(this.displaySize, displaySize);

        assertEquals(this.columnName, metaData.getColumnLabel(columnIndex));
        assertEquals(this.columnName, metaData.getColumnName(columnIndex));

        int length = metaData.getLength(columnIndex);
        assertEquals(this.columnSize, length);

        int precision = metaData.getPrecision(columnIndex);
        if (this.precision == null) {
            assertEquals(0, precision);
        } else {
            assertEquals(this.precision, precision);
        }

        int scale = metaData.getScale(columnIndex);
        if (this.scale == null) {
            assertEquals(0, scale);
        } else {
            assertEquals(this.scale, scale);
        }

        int columnType = metaData.getColumnType(columnIndex);
        assertEquals(this.dataType, JDBCType.valueOf(columnType));

        String typeName = metaData.getColumnTypeName(columnIndex);
        assertEquals(this.typeBaseName, typeName);

        String className = metaData.getColumnClassName(columnIndex);
        assertEquals(this.typeClass.getCanonicalName(), className);
    }

    public void test(TsurugiJdbcParameterMetaData metaData, int parameterIndex) throws SQLException {
        int nullable = metaData.isNullable(parameterIndex);
        assertEquals(ResultSetMetaData.columnNullableUnknown, nullable);

        boolean signed = metaData.isSigned(parameterIndex);
        assertEquals(this.signed, signed);

        int precision = metaData.getPrecision(parameterIndex);
        assertEquals(0, precision);

        int scale = metaData.getScale(parameterIndex);
        assertEquals(0, scale);

        int parameterType = metaData.getParameterType(parameterIndex);
        assertEquals(this.dataType, JDBCType.valueOf(parameterType));

        String typeName = metaData.getParameterTypeName(parameterIndex);
        assertEquals(this.typeBaseName, typeName);

        String className = metaData.getParameterClassName(parameterIndex);
        assertEquals(this.typeClass.getCanonicalName(), className);
    }
}
