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

public class TsurugiJdbcPropertyString extends TsurugiJdbcProperty {

    private String value;
    private String defaultValue;
    private Consumer<String> eventHandler;

    public TsurugiJdbcPropertyString(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyString description(String description) {
        super.description(description);
        return this;
    }

    public TsurugiJdbcPropertyString defaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public TsurugiJdbcPropertyString withEventHandler(Consumer<String> handler) {
        this.eventHandler = handler;
        return this;
    }

    @Override
    public void setStringValue(String value) {
        this.value = value;
        if (this.eventHandler != null) {
            eventHandler.accept(value);
        }
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyString) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;
    }

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

    public void ifPresent(Consumer<String> consumer) {
        String v = value();
        if (v != null) {
            consumer.accept(v);
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
