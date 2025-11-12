package com.afsun.lineage.core;

import lombok.Data;

import java.util.Locale;
import java.util.Objects;

@Data
public class ColumnRef {
    private final String database, schema, table, column;
    private final String originalDatabase, originalSchema, originalTable, originalColumn;

    private ColumnRef(String db, String sc, String tb, String col,
                      String odb, String osc, String otb, String ocol) {
        this.database = db; this.schema = sc; this.table = tb; this.column = col;
        this.originalDatabase = odb; this.originalSchema = osc; this.originalTable = otb; this.originalColumn = ocol;
    }

    public static ColumnRef of(String db, String sc, String tb, String col,
                               String odb, String osc, String otb, String ocol) {
        return new ColumnRef(
                db == null ? null : db.toLowerCase(Locale.ROOT),
                sc == null ? null : sc.toLowerCase(Locale.ROOT),
                tb.toLowerCase(Locale.ROOT),
                col.toLowerCase(Locale.ROOT),
                odb, osc, otb, ocol
        );
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnRef)) return false;
        ColumnRef c = (ColumnRef) o;
        return Objects.equals(database, c.database)
                && Objects.equals(schema, c.schema)
                && Objects.equals(table, c.table)
                && Objects.equals(column, c.column);
    }
    @Override public int hashCode() {
        return Objects.hash(database, schema, table, column);
    }
}