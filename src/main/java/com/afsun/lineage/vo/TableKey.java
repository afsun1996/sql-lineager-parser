package com.afsun.lineage.vo;

import java.util.Locale;
import java.util.Objects;

public final class TableKey {
    final String database; // 这里始终为 null（可按需改）
    final String schema;   // 这里存 ClickHouse 的 database
    final String table;

    public TableKey(String database, String schema, String table) {
        this.database = database == null ? null : database.toLowerCase(Locale.ROOT);
        this.schema = schema == null ? null : schema.toLowerCase(Locale.ROOT);
        this.table = table == null ? null : table.toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableKey)) return false;
        TableKey that = (TableKey) o;
        return Objects.equals(database, that.database)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, schema, table);
    }
}