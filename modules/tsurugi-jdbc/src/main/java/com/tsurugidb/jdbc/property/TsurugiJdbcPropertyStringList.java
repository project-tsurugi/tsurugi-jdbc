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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Tsurugi JDBC Property (list of string).
 */
public class TsurugiJdbcPropertyStringList extends TsurugiJdbcProperty {

    private List<String> value;
    private List<String> defaultValue;
    private Consumer<List<String>> changeEventHandler;

    /**
     * Creates a new instance.
     *
     * @param name property name
     */
    public TsurugiJdbcPropertyStringList(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyStringList description(String description) {
        super.description(description);
        return this;
    }

    /**
     * Set default value.
     *
     * @param defaultValue default value
     * @return this
     */
    public TsurugiJdbcPropertyStringList defaultValue(List<String> defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    /**
     * Set change event handler.
     *
     * @param handler change event handler
     * @return this
     */
    public TsurugiJdbcPropertyStringList changeEvent(Consumer<List<String>> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    /**
     * Set value.
     *
     * @param value value
     */
    public void setValue(List<String> value) {
        this.value = value;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setStringValue(String value) {
        var list = toList(value);
        setValue(list);
    }

    private List<String> toList(String value) {
        if (value == null) {
            return null;
        }

        String[] ss = value.split(",");
        var list = new ArrayList<String>(ss.length);
        for (String s : ss) {
            list.add(s.trim());
        }
        return list;
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyStringList) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public boolean isPresent() {
        return this.value != null;
    }

    /**
     * Get value.
     *
     * @return value
     */
    public List<String> value() {
        if (this.value == null) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        List<String> v = value();
        if (v == null) {
            return null;
        }
        return v.toString();
    }

    /**
     * If value is present, execute the specified action.
     *
     * @param action action
     */
    public void ifPresent(Consumer<List<String>> action) {
        List<String> v = value();
        if (v != null) {
            action.accept(v);
        }
    }

    @Override
    public String getStringDefaultValue() {
        if (this.defaultValue == null) {
            return null;
        }
        return this.defaultValue.toString();
    }

    @Override
    public String[] getChoice() {
        return null;
    }
}
