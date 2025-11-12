package com.afsun.lineage.neo4j.node;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.RelationshipProperties;
import org.springframework.data.neo4j.core.schema.TargetNode;

@RelationshipProperties
@Data
public class ToEdgeRelationship {
    @Id
    @GeneratedValue
    private Long id;

    @TargetNode
    private ColumnNodeEntity targetColumn;

    // 可以添加属性，如转换类型、权重等
    // private String transformType;
}
