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

import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/**
 * Tsurugi JDBC Property (int).
 */
public class TsurugiJdbcPropertyInt extends TsurugiJdbcProperty {

    private OptionalInt value = OptionalInt.empty();
    private OptionalInt defaultValue = OptionalInt.empty();
    private IntSupplier defaultValueSupplier = null;
    private Consumer<OptionalInt> changeEventHandler;

    /**
     * Creates a new instance.
     *
     * @param name property name
     */
    public TsurugiJdbcPropertyInt(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyInt description(String description) {
        super.description(description);
        return this;
    }

    /**
     * Set default value.
     *
     * @param defaultValue default value
     * @return this
     */
    public TsurugiJdbcPropertyInt defaultValue(int defaultValue) {
        this.defaultValue = OptionalInt.of(defaultValue);
        return this;
    }

    /**
     * Set default value supplier.
     *
     * @param supplier default value supplier
     * @return this
     */
    public TsurugiJdbcPropertyInt defaultValue(IntSupplier supplier) {
        this.defaultValueSupplier = supplier;
        return this;
    }

    /**
     * Set change event handler.
     *
     * @param handler change event handler
     * @return this
     */
    public TsurugiJdbcPropertyInt changeEvent(Consumer<OptionalInt> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    /**
     * Set value.
     *
     * @param value value
     */
    public void setValue(int value) {
        this.value = OptionalInt.of(value);

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setStringValue(String value) {
        if (value == null) {
            this.value = OptionalInt.empty();
        } else {
            this.value = OptionalInt.of(Integer.parseInt(value));
        }

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyInt) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;
        this.defaultValueSupplier = from.defaultValueSupplier;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(this.value);
        }
    }

    @Override
    public boolean isPresent() {
        return this.value.isPresent();
    }

    /**
     * Get value.
     *
     * @return value
     */
    public OptionalInt value() {
        if (this.value.isEmpty()) {
            return defaultValue();
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

    /**
     * If value is present, execute the specified action.
     *
     * @param action action
     */
    public void ifPresent(IntConsumer action) {
        OptionalInt v = value();
        v.ifPresent(action);
    }

    /**
     * Get default value.
     *
     * @return default value
     */
    protected OptionalInt defaultValue() {
        if (this.defaultValueSupplier != null) {
            return OptionalInt.of(defaultValueSupplier.getAsInt());
        }
        return this.defaultValue;
    }

    @Override
    public String getStringDefaultValue() {
        OptionalInt v = defaultValue();
        if (v.isEmpty()) {
            return null;
        }
        return Integer.toString(v.getAsInt());
    }

    @Override
    public String[] getChoice() {
        return null;
    }
}
