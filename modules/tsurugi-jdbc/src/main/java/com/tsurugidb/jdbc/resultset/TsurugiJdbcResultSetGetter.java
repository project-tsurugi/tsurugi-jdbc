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
import java.text.MessageFormat;
import java.util.EnumMap;
import java.util.Map;

import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;

public class TsurugiJdbcResultSetGetter {

    private static final Map<AtomType, TsurugiJdbcResultSetGetter> ATOM_TYPE_GETTER_MAP;
    static {
        var map = new EnumMap<AtomType, TsurugiJdbcResultSetGetter>(AtomType.class);
        put(map, AtomType.BOOLEAN, (o, rs) -> rs.fetchBooleanValue());
        put(map, AtomType.INT4, (o, rs) -> rs.fetchInt4Value());
        put(map, AtomType.INT8, (o, rs) -> rs.fetchInt8Value());
        put(map, AtomType.FLOAT4, (o, rs) -> rs.fetchFloat4Value());
        put(map, AtomType.FLOAT8, (o, rs) -> rs.fetchFloat8Value());
        put(map, AtomType.DECIMAL, (o, rs) -> rs.fetchDecimalValue());
        put(map, AtomType.CHARACTER, (o, rs) -> rs.fetchCharacterValue());
        put(map, AtomType.OCTET, (o, rs) -> rs.fetchOctetValue());
        put(map, AtomType.DATE, (o, rs) -> rs.fetchDateValue());
        put(map, AtomType.TIME_OF_DAY, (o, rs) -> rs.fetchTimeOfDayValue());
        put(map, AtomType.TIME_POINT, (o, rs) -> rs.fetchTimePointValue());
        put(map, AtomType.TIME_OF_DAY_WITH_TIME_ZONE, (o, rs) -> rs.fetchTimeOfDayWithTimeZoneValue());
        put(map, AtomType.TIME_POINT_WITH_TIME_ZONE, (o, rs) -> rs.fetchTimePointWithTimeZoneValue());
        put(map, AtomType.BLOB, TsurugiJdbcResultSetGetter::fetchBlob);
        put(map, AtomType.CLOB, TsurugiJdbcResultSetGetter::fetchClob);
        ATOM_TYPE_GETTER_MAP = map;
    }

    private static void put(Map<AtomType, TsurugiJdbcResultSetGetter> map, AtomType atomType, Getter getter) {
        map.put(atomType, new TsurugiJdbcResultSetGetter(getter));
    }

    static TsurugiJdbcBlobReference fetchBlob(TsurugiJdbcResultSet ownerResultSet, ResultSet lowRs) throws IOException, InterruptedException, ServerException {
        var lowBlob = lowRs.fetchBlob();
        return new TsurugiJdbcBlobReference(ownerResultSet, lowBlob);
    }

    static TsurugiJdbcClobReference fetchClob(TsurugiJdbcResultSet ownerResultSet, ResultSet lowRs) throws IOException, InterruptedException, ServerException {
        var lowClob = lowRs.fetchClob();
        return new TsurugiJdbcClobReference(ownerResultSet, lowClob);
    }

    public static TsurugiJdbcResultSetGetter of(Column column) {
        var atomType = column.getAtomType();
        var getter = ATOM_TYPE_GETTER_MAP.get(atomType);
        if (getter == null) {
            throw new UnsupportedOperationException(MessageFormat.format("AtomType.{0} is not supported", atomType));
        }
        return getter;
    }

    @FunctionalInterface
    private interface Getter {
        public Object fetchValue(TsurugiJdbcResultSet ownerResultSet, ResultSet lowRs) throws IOException, InterruptedException, ServerException;
    }

    private final Getter getter;

    private TsurugiJdbcResultSetGetter(Getter getter) {
        this.getter = getter;
    }

    public Object fetchValue(TsurugiJdbcResultSet ownerResultSet, ResultSet lowRs) throws IOException, InterruptedException, ServerException {
        if (lowRs.isNull()) {
            return null;
        }

        return getter.fetchValue(ownerResultSet, lowRs);
    }
}
