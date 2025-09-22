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

import java.sql.SQLException;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

public abstract class TsurugiJdbcProperty {

    private final String name;
    private String description;

    public TsurugiJdbcProperty(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public TsurugiJdbcProperty description(String description) {
        this.description = description;
        return this;
    }

    public String description() {
        return this.description;
    }

    public abstract void setStringValue(String value) throws SQLException;

    @OverridingMethodsMustInvokeSuper
    public void setFrom(TsurugiJdbcProperty fromProperty) {
        this.description = fromProperty.description;
    }

    public abstract String getStringValue();

    public abstract String getStringDefaultValue();

    public abstract @Nullable String[] getChoice();
}
