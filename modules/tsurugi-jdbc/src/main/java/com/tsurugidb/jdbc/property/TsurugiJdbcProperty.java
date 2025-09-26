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

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Tsurugi JDBC Property.
 */
public abstract class TsurugiJdbcProperty {

    private final String name;
    private String description;

    /**
     * Creates a new instance.
     *
     * @param name property name
     */
    public TsurugiJdbcProperty(String name) {
        this.name = name;
    }

    /**
     * Get property name.
     *
     * @return property name
     */
    public String name() {
        return this.name;
    }

    /**
     * Set description.
     *
     * @param description description
     * @return this
     */
    public TsurugiJdbcProperty description(String description) {
        this.description = description;
        return this;
    }

    /**
     * Get description.
     *
     * @return description
     */
    public String description() {
        return this.description;
    }

    /**
     * Set string value.
     *
     * @param value string value
     */
    public abstract void setStringValue(String value);

    /**
     * Set from another property.
     *
     * @param fromProperty source property
     */
    @OverridingMethodsMustInvokeSuper
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        this.description = fromProperty.description;
    }

    /**
     * Is value present?
     *
     * @return true: present, false: not present
     */
    public abstract boolean isPresentValue();

    /**
     * Get string value.
     *
     * @return string value
     */
    public abstract String getStringValue();

    /**
     * Get default string value.
     *
     * @return default string value
     */
    public abstract String getStringDefaultValue();

    /**
     * Get choice values.
     *
     * @return choice values
     */
    public abstract @Nullable String[] getChoice();
}
