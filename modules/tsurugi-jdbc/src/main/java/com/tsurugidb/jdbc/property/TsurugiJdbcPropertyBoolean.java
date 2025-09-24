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

public class TsurugiJdbcPropertyBoolean extends TsurugiJdbcProperty {

    private Boolean value;
    private boolean defaultValue;
    private Consumer<Boolean> changeEventHandler;

    public TsurugiJdbcPropertyBoolean(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyBoolean description(String description) {
        super.description(description);
        return this;
    }

    public TsurugiJdbcPropertyBoolean defaultValue(boolean defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public TsurugiJdbcPropertyBoolean changeEvent(Consumer<Boolean> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    public void setValue(boolean value) {
        this.value = value;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setStringValue(String value) {
        if (value == null) {
            this.value = null;
        } else {
            this.value = Boolean.parseBoolean(value);
        }

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyBoolean) fromProperty;
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

    public boolean value() {
        if (this.value == null) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        boolean v = value();
        return Boolean.toString(v);
    }

    @Override
    public String getStringDefaultValue() {
        return Boolean.toString(this.defaultValue);
    }

    @Override
    public String[] getChoice() {
        return new String[] { "true", "false" };
    }
}
