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

/**
 * Tsurugi JDBC Type Info.
 */
public class TsurugiJdbcTypeInfo implements Cloneable {

    /**
     * Create type info.
     *
     * @param typeName type name
     * @param dataType data type
     * @return type info
     */
    public static TsurugiJdbcTypeInfo of(String typeName, JDBCType dataType) {
        return new TsurugiJdbcTypeInfo(typeName, dataType);
    }

    private String typeName;
    private JDBCType dataType;
    private int precision;
    private String literalPrefix;
    private String literalSuffix;
    private String createParams;
    private short nullable = DatabaseMetaData.typeNullable;
    private boolean caseSensitive = true;
    private short searchable = DatabaseMetaData.typePredNone;
    private boolean unsignedAttribute = true;
    private boolean fixedPrecScale = false;
    private boolean autoIncrement = false;
    private String localTypeName;
    private short minimumScale;
    private short maximumScale;
    private int sqlDataType;
    private int sqlDatetimeSub;
    private int numPrecRaddix;

    /**
     * Creates a new instance.
     *
     * @param typeName type name
     * @param dataType data type
     */
    public TsurugiJdbcTypeInfo(String typeName, JDBCType dataType) {
        this.typeName = typeName;
        this.dataType = dataType;
    }

    /**
     * Get SQL type.
     *
     * @return SQL type (java.sql.Types)
     */
    public Integer getSqlType() {
        return dataType.getVendorTypeNumber();
    }

    /**
     * Set precision.
     *
     * @param precision precision
     * @return this
     */
    public TsurugiJdbcTypeInfo precision(int precision) {
        this.precision = precision;
        return this;
    }

    /**
     * Set literal prefix/suffix.
     *
     * @param prefix literal prefix
     * @param suffix literal suffix
     * @return this
     */
    public TsurugiJdbcTypeInfo literalPrefix(String prefix, String suffix) {
        this.literalPrefix = prefix;
        this.literalSuffix = suffix;
        return this;
    }

    /**
     * Set searchable.
     *
     * @param searchable searchable
     * @return this
     */
    public TsurugiJdbcTypeInfo searchable(int searchable) {
        this.searchable = (short) searchable;
        return this;
    }

    /**
     * Set unsigned attribute.
     *
     * @param unsignedAttribute unsigned attribute
     * @return this
     */
    public TsurugiJdbcTypeInfo unsignedAttribute(boolean unsignedAttribute) {
        this.unsignedAttribute = unsignedAttribute;
        return this;
    }

    /**
     * Set auto increment.
     *
     * @param autoIncrement auto increment
     * @return this
     */
    public TsurugiJdbcTypeInfo autoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
        return this;
    }

    /**
     * Set scale.
     *
     * @param minimumScale minimum scale
     * @param maximumScale maximum scale
     * @return this
     */
    public TsurugiJdbcTypeInfo minimumScale(short minimumScale, short maximumScale) {
        this.minimumScale = minimumScale;
        this.maximumScale = maximumScale;
        return this;
    }

    /**
     * Convert to values.
     *
     * @return values
     */
    public Object[] toValues() {
        Object[] values = { //
                typeName, //
                dataType.getVendorTypeNumber(), //
                precision, //
                literalPrefix, //
                literalSuffix, //
                createParams, //
                nullable, //
                caseSensitive, //
                searchable, //
                unsignedAttribute, //
                fixedPrecScale, //
                autoIncrement, //
                localTypeName, //
                minimumScale, //
                maximumScale, //
                sqlDataType, //
                sqlDatetimeSub, //
                numPrecRaddix //
        };
        return values;
    }

    @Override
    protected TsurugiJdbcTypeInfo clone() {
        try {
            return (TsurugiJdbcTypeInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    /**
     * Clone.
     *
     * @param typeName type name
     * @param dataType data type
     * @return type info
     */
    public TsurugiJdbcTypeInfo clone(String typeName, JDBCType dataType) {
        var clone = clone();
        clone.typeName = typeName;
        clone.dataType = dataType;
        return clone;
    }

}
