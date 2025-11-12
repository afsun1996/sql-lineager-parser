package com.afsun.lineage.graph;
import lombok.Data;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;

import java.util.Locale;
import java.util.Objects;

@Data
public class ColumnNode {
    @Id
    @GeneratedValue
    private Long id;
    @Property("database")
    private final String database;
    @Property("schema")
    private final String schema;
    @Property("tableName")
    private final String table;
    @Property("name")
    private final String column;
    private final String originalDatabase;
    private final String originalSchema;
    private final String originalTable;
    private final String originalColumn;

    public ColumnNode(String database, String schema, String table, String column,
                      String originalDatabase, String originalSchema, String originalTable, String originalColumn) {
        this.database = toLowerOrNull(database);
        this.schema = toLowerOrNull(schema);
        this.table = toLowerOrNull(table);
        this.column = toLowerOrNull(column);
        this.originalDatabase = originalDatabase;
        this.originalSchema = originalSchema;
        this.originalTable = originalTable;
        this.originalColumn = originalColumn;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnNode)) return false;
        ColumnNode that = (ColumnNode) o;
        return Objects.equals(database, that.database)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table)
                && Objects.equals(column, that.column);
    }



    private static String toLowerOrNull(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }
}