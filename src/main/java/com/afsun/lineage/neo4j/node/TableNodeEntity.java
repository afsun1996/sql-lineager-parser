package com.afsun.lineage.neo4j.node;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.*;
import java.util.*;

@Node("Table") // 在Neo4j中节点的标签为"Table"
@Data // 使用Lombok
public class TableNodeEntity {
    @Id
    @GeneratedValue
    private Long id; // Neo4j 生成的内部ID

    @Property("database")
    private String database;
    @Property("schema")
    private String schema;
    @Property("name") // 注意：对应原TableNode中的table字段
    private String table;

    // 原始信息属性
    private String originalDatabase;
    private String originalSchema;
    private String originalTable;

    // 定义一个唯一约束，防止重复创建相同表节点
    @CompositeProperty()
    private List<String> naturalKey; // 例如: Arrays.asList(database, schema, table)

    public TableNodeEntity(String database, String schema, String table, String originalDatabase, String originalSchema, String originalTable) {
        this.database = database != null ? database.toLowerCase() : null;
        this.schema = schema != null ? schema.toLowerCase() : null;
        this.table = table != null ? table.toLowerCase() : null;
        this.originalDatabase = originalDatabase;
        this.originalSchema = originalSchema;
        this.originalTable = originalTable;
        this.naturalKey = Arrays.asList(this.database, this.schema, this.table);
    }

}
