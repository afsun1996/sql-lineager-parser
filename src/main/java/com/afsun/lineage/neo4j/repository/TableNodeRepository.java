package com.afsun.lineage.neo4j.repository;

import com.afsun.lineage.neo4j.node.ColumnNodeEntity;
import com.afsun.lineage.neo4j.node.TableNodeEntity;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import java.util.Optional;

public interface TableNodeRepository extends Neo4jRepository<TableNodeEntity, Long> {
    // 根据业务键查找，用于避免重复插入
    Optional<TableNodeEntity> findByDatabaseAndSchemaAndTable(String database, String schema, String table);
}

