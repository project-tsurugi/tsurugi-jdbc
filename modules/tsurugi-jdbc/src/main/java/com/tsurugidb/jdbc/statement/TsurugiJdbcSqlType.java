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

import static java.sql.Types.BIT;
import static java.sql.Types.BOOLEAN;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class TsurugiJdbcSqlType {

    public static AtomType toLowAtomType(int sqlType) throws SQLException {
        switch (sqlType) {
        case BOOLEAN:
        case BIT:
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
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported sqlType {0}", sqlType));
        }
    }
}
