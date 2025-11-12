package com.afsun.lineage.neo4j.node;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.*;

@RelationshipProperties
@Data
public class OwnerEdgeRelationship {
    @Id
    @GeneratedValue
    private Long id;

    @TargetNode // 关系指向的目标节点
    private TableNodeEntity table;
}

