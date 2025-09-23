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

    private final Class<E> enumType;
    private E value;
    private E defaultValue;
    private Consumer<E> changeEventHandler;

    public TsurugiJdbcPropertyEnum(Class<E> type, String name) {
        super(name);
        this.enumType = type;
    }

    @Override
    public TsurugiJdbcPropertyEnum<E> description(String description) {
        super.description(description);
        return this;
    }

    public TsurugiJdbcPropertyEnum<E> defaultValue(E defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public TsurugiJdbcPropertyEnum<E> changeEvent(Consumer<E> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    public void setValue(E value) {
        this.value = value;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setStringValue(String value) {
        if (value == null) {
            setValue(null);
            return;
        }

        E[] constants = enumType.getEnumConstants();
        for (var c : constants) {
            if (value.equalsIgnoreCase(c.name())) {
                setValue(c);
                return;
            }
        }

        throw new IllegalArgumentException("specified one of " + Arrays.toString(constants));
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        @SuppressWarnings("unchecked")
        var from = (TsurugiJdbcPropertyEnum<E>) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    public E value() {
        if (this.value == null) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        E v = value();
        if (v == null) {
            return null;
        }
        return v.name();
    }

    public void ifPresent(Consumer<E> consumer) {
        E v = value();
        if (v != null) {
            consumer.accept(v);
        }
    }

    @Override
    public String getStringDefaultValue() {
        if (this.defaultValue == null) {
            return null;
        }
        return this.defaultValue.name();
    }

    @Override
    public String[] getChoice() {
        E[] constants = enumType.getEnumConstants();

        var result = new String[constants.length];
        for (int i = 0; i < constants.length; i++) {
            result[i] = constants[i].name();
        }
        return result;
    }
}
