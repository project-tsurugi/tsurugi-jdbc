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
package com.tsurugidb.jdbc.property;

import java.sql.SQLException;
import java.util.OptionalInt;

public class TsurugiJdbcPropertyInt extends TsurugiJdbcProperty {

    private OptionalInt value = OptionalInt.empty();
    private OptionalInt defaultValue = OptionalInt.empty();

    public TsurugiJdbcPropertyInt(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyInt description(String description) {
        super.description(description);
        return this;
    }

    public TsurugiJdbcPropertyInt defaultValue(int defaultValue) {
        this.defaultValue = OptionalInt.of(defaultValue);
        return this;
    }

    public void setValue(int value) {
        this.value = OptionalInt.of(value);
    }

    @Override
    public void setStringValue(String value) throws SQLException {
        if (value == null) {
            this.value = OptionalInt.empty();
            return;
        }

        try {
            setValue(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyInt) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;
    }

    public OptionalInt value() {
        if (this.value.isEmpty()) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        OptionalInt v = value();
        if (v.isEmpty()) {
            return null;
        }
        return Integer.toString(v.getAsInt());
    }

    @Override
    public String getStringDefaultValue() {
        if (this.defaultValue.isEmpty()) {
            return null;
        }
        return Integer.toString(defaultValue.getAsInt());
    }
}
