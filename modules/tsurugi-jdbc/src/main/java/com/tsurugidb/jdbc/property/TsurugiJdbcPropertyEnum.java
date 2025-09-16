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

import java.util.Arrays;
import java.util.function.Consumer;

public class TsurugiJdbcPropertyEnum<E extends Enum<E>> extends TsurugiJdbcProperty {

    private final Class<E> type;
    private E value;
    private Consumer<E> eventHandler;

    public TsurugiJdbcPropertyEnum(Class<E> type, String name) {
        super(name);
        this.type = type;
    }

    public TsurugiJdbcPropertyEnum<E> withEventHandler(Consumer<E> handler) {
        this.eventHandler = handler;
        return this;
    }

    public void setValue(E value) {
        this.value = value;
        if (this.eventHandler != null) {
            eventHandler.accept(value);
        }
    }

    @Override
    public void setStringValue(String value) {
        if (value == null) {
            setValue(null);
            return;
        }

        E[] constants = type.getEnumConstants();
        for (var c : constants) {
            if (value.equals(c.name())) {
                setValue(c);
                return;
            }
        }
        throw new IllegalArgumentException("specified one of " + Arrays.toString(constants));
    }

    @Override
    public void setFrom(TsurugiJdbcProperty property) {
        @SuppressWarnings("unchecked")
        var from = (TsurugiJdbcPropertyEnum<E>) property;
        this.value = from.value();
    }

    public E value() {
        return this.value;
    }

    @Override
    public String getStringValue() {
        if (this.value == null) {
            return null;
        }
        return value.name();
    }

    public void ifPresent(Consumer<E> consumer) {
        if (this.value != null) {
            consumer.accept(value);
        }
    }
}
