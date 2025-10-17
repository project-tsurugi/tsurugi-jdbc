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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.tsurugidb.jdbc.factory.GetFactory;

/**
 * Tsurugi JDBC Fixed ResultSet.
 */
public class FixedResultSet extends AbstractResultSet {

    private final List<FixedResultSetColumn> columnList;
    private final List<Object[]> valuesList;

    private FixedResultSetMetaData resultSetMetaData = null;

    private int fetchDirection = FETCH_FORWARD;
    private int fetchSize = 0;

    private int currentRowNumber = 0;

    /**
     * Creates a new instance.
     *
     * @param owner      factory holder
     * @param columnList column list
     * @param valuesList values list
     */
    public FixedResultSet(GetFactory owner, List<FixedResultSetColumn> columnList, List<Object[]> valuesList) {
        super(owner);
        this.columnList = Objects.requireNonNull(columnList);
        this.valuesList = Objects.requireNonNull(valuesList);
    }

    @Override
    public boolean next() throws SQLException {
        return relative(+1);
    }

    @Override
    public FixedResultSetMetaData getMetaData() throws SQLException {
        if (this.resultSetMetaData == null) {
            this.resultSetMetaData = new FixedResultSetMetaData(this, columnList);
        }
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object value;
        try {
            var values = valuesList.get(currentRowNumber - 1);
            value = values[columnIndex - 1];
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getObject error", e);
        }

        this.wasNull = (value == null);
        return value;
    }

    @Override
    protected Map<String, Integer> createColumnIndexMap() throws SQLException {
        var map = new HashMap<String, Integer>(columnList.size());

        for (int i = 0; i < columnList.size(); i++) {
            var column = columnList.get(i);
            String name = column.name();
            if (name != null) {
                map.put(name, i + 1);
            }
        }

        return map;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.currentRowNumber <= 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.currentRowNumber > valuesList.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.currentRowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return this.currentRowNumber == valuesList.size();
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.currentRowNumber = 0;
    }

    @Override
    public void afterLast() throws SQLException {
        this.currentRowNumber = valuesList.size();
    }

    @Override
    public boolean first() throws SQLException {
        return absolute(1);
    }

    @Override
    public boolean last() throws SQLException {
        return absolute(-1);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        int maxNumber = valuesList.size() + 1;

        int nextNumber;
        if (row >= 0) {
            nextNumber = row;
        } else {
            nextNumber = maxNumber + row;
        }

        if (nextNumber < 0) {
            nextNumber = 0;
        } else if (nextNumber > maxNumber) {
            nextNumber = maxNumber;
        }

        this.currentRowNumber = nextNumber;
        return 1 <= nextNumber && nextNumber <= valuesList.size();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return absolute(this.currentRowNumber + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return relative(-1);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }
}
