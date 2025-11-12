package com.afsun.lineage.graph;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;

import java.util.Locale;
import java.util.Objects;

@Data
public class TableNode {
    @Id
    @GeneratedValue
    private Long id; // Neo4j 生成的内部ID

    @Property("database")
    private final String database;

    @Property("schema")
    private final String schema;

    @Property("name")
    private final String table;

    private final String originalDatabase;

    private final String originalSchema;

    private final String originalTable;

    public TableNode(String database, String schema, String table,
                     String originalDatabase, String originalSchema, String originalTable) {
        this.database = toLowerOrNull(database);
        this.schema = toLowerOrNull(schema);
        this.table = toLowerOrNull(table);
        this.originalDatabase = originalDatabase;
        this.originalSchema = originalSchema;
        this.originalTable = originalTable;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableNode)) return false;
        TableNode that = (TableNode) o;
        return Objects.equals(database, that.database)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, schema, table);
    }


    private static String toLowerOrNull(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }
}