/*
 * Copyright 2025-2026 Project Tsurugi.
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

import java.net.URI;
import java.util.function.Consumer;

/**
 * Tsurugi JDBC Property (URI).
 */
public class TsurugiJdbcPropertyUri extends TsurugiJdbcProperty {

    private URI value;
    private URI defaultValue;
    private Consumer<URI> changeEventHandler;

    /**
     * Creates a new instance.
     *
     * @param name property name
     */
    public TsurugiJdbcPropertyUri(String name) {
        super(name);
    }

    @Override
    public TsurugiJdbcPropertyUri description(String description) {
        super.description(description);
        return this;
    }

    /**
     * Set default value.
     *
     * @param defaultValue default value
     * @return this
     */
    public TsurugiJdbcPropertyUri defaultValue(URI defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    /**
     * Set change event handler.
     *
     * @param handler change event handler
     * @return this
     */
    public TsurugiJdbcPropertyUri changeEvent(Consumer<URI> handler) {
        this.changeEventHandler = handler;
        return this;
    }

    /**
     * Set value.
     *
     * @param value value
     */
    public void setValue(URI value) {
        URI old = value();
        this.value = value;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(old);
        }
    }

    @Override
    public void setStringValue(String value) {
        if (value == null) {
            setValue(null);
            return;
        }

        URI uri = URI.create(value);
        setValue(uri);
    }

    @Override
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        super.setFrom(fromProperty);

        var from = (TsurugiJdbcPropertyUri) fromProperty;
        this.value = from.value;
        this.defaultValue = from.defaultValue;

        if (this.changeEventHandler != null) {
            changeEventHandler.accept(null);
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
    public URI value() {
        if (this.value == null) {
            return this.defaultValue;
        }
        return this.value;
    }

    @Override
    public String getStringValue() {
        URI v = value();
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
    public void ifPresent(Consumer<URI> action) {
        URI v = value();
        if (v != null) {
            action.accept(v);
        }
    }

    @Override
    public String getStringDefaultValue() {
        URI v = this.defaultValue;
        if (v == null) {
            return null;
        }
        return v.toString();
    }

    @Override
    public String[] getChoice() {
        return null;
    }
}
