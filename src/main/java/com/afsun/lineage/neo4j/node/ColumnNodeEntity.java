package com.afsun.lineage.neo4j.node;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.*;

import java.util.List;
import java.util.Set;

@Node("Column")
@Data
public class ColumnNodeEntity {
    @Id
    @GeneratedValue
    private Long id;

    @Property("database")
    private String database;
    @Property("schema")
    private String schema;
    @Property("tableName")
    private String table;
    @Property("name")
    private String column;

    // 原始信息属性
    private String originalDatabase;
    // ... 其他original字段

    @CompositeProperty()
    private List<String> naturalKey; // Arrays.asList(database, schema, table, column)

    @Relationship(type = "BELONGS_TO", direction = Relationship.Direction.OUTGOING)
    private OwnerEdgeRelationship ownerEdge;
    // 构造方法，equals，hashCode 等

    // 一个Column节点可以指向多个其他Column节点，关系类型为"LINKS_TO"
    @Relationship(type = "LINKS_TO", direction = Relationship.Direction.OUTGOING)
    private Set<ToEdgeRelationship> dataLineage;
}

