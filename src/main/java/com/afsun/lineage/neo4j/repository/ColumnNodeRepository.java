package com.afsun.lineage.neo4j.repository;

import com.afsun.lineage.neo4j.node.ColumnNodeEntity;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Optional;

public interface ColumnNodeRepository extends Neo4jRepository<ColumnNodeEntity, Long> {
    Optional<ColumnNodeEntity> findByDatabaseAndSchemaAndTableAndColumn(String database, String schema, String table, String column);
}
