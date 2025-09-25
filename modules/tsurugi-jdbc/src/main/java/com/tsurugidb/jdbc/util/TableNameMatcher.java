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
package com.tsurugidb.jdbc.util;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Table name matcher.
 */
public class TableNameMatcher {

    /**
     * Create TableNameMatcher.
     *
     * @param catalog          database name
     * @param schemaPattern    schema pattern
     * @param tableNamePattern table name pattern
     * @param types            types
     * @return TableNameMatcher
     */
    public static TableNameMatcher of(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        Set<String> set;
        if (types != null) {
            set = Set.of(types);
        } else {
            set = null;
        }

        return new TableNameMatcher(catalog, toPattern(schemaPattern), toPattern(tableNamePattern), set);
    }

    private static Pattern toPattern(String likePattern) {
        if (likePattern == null || likePattern.isEmpty()) {
            return null;
        }

        String regex = likePattern //
                .replaceAll(Pattern.quote("."), Pattern.quote(".")) //
                .replaceAll(Pattern.quote("%"), ".*") //
                .replaceAll(Pattern.quote("_"), ".") //
        ;
        return Pattern.compile(regex);
    }

    // private String catalog;
    // private Pattern schemaPattern;
    private Pattern tableNamePattern;
    private Pattern columnNamePattern;
    private Set<String> types;

    /**
     * Creates a new instance.
     *
     * @param catalog          database name
     * @param schemaPattern    schema pattern
     * @param tableNamePattern table name pattern
     * @param types            types
     */
    public TableNameMatcher(String catalog, Pattern schemaPattern, Pattern tableNamePattern, Set<String> types) {
        // this.catalog = catalog;
        // this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.types = types;
    }

    /**
     * Matches table name and type.
     *
     * @param tableName table name
     * @param type      type
     * @return true if matches, false otherwise
     */
    public boolean matches(String tableName, String type) {
        if (tableNamePattern != null) {
            if (!tableNamePattern.matcher(tableName).matches()) {
                return false;
            }
        }

        if (this.types != null) {
            if (!types.contains(type)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Set column name pattern.
     *
     * @param columnNamePattern column name pattern
     * @return this
     */
    public TableNameMatcher columnNamePattern(String columnNamePattern) {
        this.columnNamePattern = toPattern(columnNamePattern);
        return this;
    }

    /**
     * Matches column name.
     *
     * @param columnName column name
     * @return true if matches, false otherwise
     */
    public boolean matchesColumnName(String columnName) {
        if (columnNamePattern != null) {
            if (!columnNamePattern.matcher(columnName).matches()) {
                return false;
            }
        }

        return true;
    }
}
