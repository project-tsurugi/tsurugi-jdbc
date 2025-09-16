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
    private Consumer<String> eventHandler;

    public TsurugiJdbcPropertyString(String name) {
        super(name);
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
    public void setFrom(TsurugiJdbcProperty property) {
        var from = (TsurugiJdbcPropertyString) property;
        setStringValue(from.value());
    }

    public String value() {
        return this.value;
    }

    @Override
    public String getStringValue() {
        return this.value;
    }

    public void ifPresent(Consumer<String> consumer) {
        if (this.value != null) {
            consumer.accept(value);
        }
    }
}
