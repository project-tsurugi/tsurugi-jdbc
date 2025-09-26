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

import java.util.function.Consumer;

/**
 * Tsurugi JDBC Property (String).
 */
public class TsurugiJdbcPropertyString extends TsurugiJdbcProperty {

    private String value;
    private String defaultValue;
    private Consumer<String> changeEventHandler;

    /**
     * Creates a new instance.
     *
     * @param name property name
     */
    public TsurugiJdbcPropertyString(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyString description(String description) {
        super.description(description);
        return this;
    }

    /**
     * Set default value.
     *
     * @param defaultValue default value
     * @return this
     */
    public TsurugiJdbcPropertyString defaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    /**
     * Set change event handler.
     *
     * @param handler change event handler
     * @return this
     */
    public TsurugiJdbcPropertyString changeEvent(Consumer<String> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    /**
     * Set value.
     *
     * @param value value
     */
    public void setValue(String value) {
        this.value = value;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setStringValue(String value) {
        setValue(value);
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyString) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public boolean isPresentValue() {
        return this.value != null;
    }

    /**
     * Get value.
     *
     * @return value
     */
    public String value() {
        if (this.value == null) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        return value();
    }

    /**
     * If value is present, execute the specified action.
     *
     * @param action action
     */
    public void ifPresent(Consumer<String> action) {
        String v = value();
        if (v != null) {
            action.accept(v);
        }
    }

    @Override
    public String getStringDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public String[] getChoice() {
        return null;
    }
}
