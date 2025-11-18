package com.afsun.lineage.neo4j.service;

import com.afsun.lineage.core.LineageGraph;
import com.afsun.lineage.graph.ColumnNode;
import com.afsun.lineage.graph.OwnerEdge;
import com.afsun.lineage.graph.TableNode;
import com.afsun.lineage.graph.ToEdge;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class LineageService {

    private final Neo4jClient neo4jClient;

    // 批处理大小，可配置
    private static final int BATCH_SIZE = 100;

    public LineageService(Neo4jClient neo4jClient) {
        this.neo4jClient = neo4jClient;
    }

    @Transactional
    public void saveLineageGraph(LineageGraph graph) {
        if (graph == null) {
            return;
        }
        // 1. 批量保存Table节点
        saveTables(new ArrayList<>(graph.getTables()));
        // 2. 批量保存Column节点和BELONGS_TO关系
        saveColumnsWithBelongsTo(new ArrayList<>(graph.getOwnerEdges()));
        // 3. 批量创建LINKS_TO关系
        saveColumnLineage(new ArrayList<>(graph.getToEdges()));
    }

    private void saveTables(List<TableNode> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }

        String batchQuery = "            UNWIND $tables AS tbl\n" +
                "            MERGE (t:Table {\n" +
                "                database: COALESCE(tbl.database, 'UNKNOWN'),\n" +
                "                schema: COALESCE(tbl.schema, 'UNKNOWN'),\n" +
                "                name: tbl.name\n" +
                "            })\n" +
                "            SET t.originalDatabase = tbl.originalDatabase,\n" +
                "                t.originalSchema = tbl.originalSchema,\n" +
                "                t.originalTable = tbl.originalTable";

        // 分批处理
        for (int i = 0; i < tables.size(); i += BATCH_SIZE) {
            List<TableNode> batch = tables.subList(i, Math.min(i + BATCH_SIZE, tables.size()));
            List<Map<String, Object>> tableData = batch.stream()
                    .map(this::tableNodeToMap)
                    .collect(Collectors.toList());

            neo4jClient.query(batchQuery)
                    .bind(tableData).to("tables")
                    .run();
        }
    }

    private void saveColumnsWithBelongsTo(List<OwnerEdge> ownerEdges) {
        if (ownerEdges == null || ownerEdges.isEmpty()) {
            return;
        }

        String batchQuery = "            UNWIND $edges AS edge\n" +
                "            MERGE (col:Column {\n" +
                "                database: COALESCE(edge.colDatabase, 'UNKNOWN'),\n" +
                "                schema: COALESCE(edge.colSchema, 'UNKNOWN'),\n" +
                "                tableName: edge.colTable,\n" +
                "                name: edge.colName\n" +
                "            })\n" +
                "            SET col.originalDatabase = edge.colOrigDatabase,\n" +
                "                col.originalSchema = edge.colOrigSchema,\n" +
                "                col.originalTable = edge.colOrigTable,\n" +
                "                col.originalColumn = edge.colOrigColumn\n" +
                "            WITH col, edge\n" +
                "            MERGE (t:Table {\n" +
                "                database: COALESCE(edge.tblDatabase, 'UNKNOWN'),\n" +
                "                schema: COALESCE(edge.tblSchema, 'UNKNOWN'),\n" +
                "                name: edge.tblName\n" +
                "            })\n" +
                "            MERGE (col)-[:BELONGS_TO]->(t)";

        // 分批处理
        for (int i = 0; i < ownerEdges.size(); i += BATCH_SIZE) {
            List<OwnerEdge> batch = ownerEdges.subList(i, Math.min(i + BATCH_SIZE, ownerEdges.size()));
            List<Map<String, Object>> edgeData = batch.stream()
                    .map(this::ownerEdgeToMap)
                    .collect(Collectors.toList());

            neo4jClient.query(batchQuery)
                    .bind(edgeData).to("edges")
                    .run();
        }
    }

    private void saveColumnLineage(List<ToEdge> toEdges) {
        if (toEdges == null || toEdges.isEmpty()) {
            return;
        }

        String batchQuery = "            UNWIND $edges AS edge\n" +
                "            MERGE (src:Column {\n" +
                "                database: COALESCE(edge.srcDatabase, 'UNKNOWN'),\n" +
                "                schema: COALESCE(edge.srcSchema, 'UNKNOWN'),\n" +
                "                tableName: edge.srcTable,\n" +
                "                name: edge.srcName\n" +
                "            })\n" +
                "            MERGE (dst:Column {\n" +
                "                database: COALESCE(edge.dstDatabase, 'UNKNOWN'),\n" +
                "                schema: COALESCE(edge.dstSchema, 'UNKNOWN'),\n" +
                "                tableName: edge.dstTable,\n" +
                "                name: edge.dstName\n" +
                "            })\n" +
                "            MERGE (src)-[:LINKS_TO]->(dst)";

        // 分批处理
        for (int i = 0; i < toEdges.size(); i += BATCH_SIZE) {
            List<ToEdge> batch = toEdges.subList(i, Math.min(i + BATCH_SIZE, toEdges.size()));
            List<Map<String, Object>> edgeData = batch.stream()
                    .map(this::toEdgeToMap)
                    .collect(Collectors.toList());

            neo4jClient.query(batchQuery)
                    .bind(edgeData).to("edges")
                    .run();
        }
    }

    // 辅助方法：将TableNode转为Map
    private Map<String, Object> tableNodeToMap(TableNode tbl) {
        Map<String, Object> map = new HashMap<>();
        map.put("database", tbl.getDatabase());
        map.put("schema", tbl.getSchema());
        map.put("name", tbl.getTable());
        map.put("originalDatabase", tbl.getOriginalDatabase());
        map.put("originalSchema", tbl.getOriginalSchema());
        map.put("originalTable", tbl.getOriginalTable());
        return map;
    }

    // 辅助方法：将OwnerEdge转为Map
    private Map<String, Object> ownerEdgeToMap(OwnerEdge edge) {
        ColumnNode col = edge.getFrom();
        TableNode tbl = edge.getTo();

        Map<String, Object> map = new HashMap<>();
        // Column信息
        map.put("colDatabase", col.getDatabase());
        map.put("colSchema", col.getSchema());
        map.put("colTable", col.getTable());
        map.put("colName", col.getColumn());
        map.put("colOrigDatabase", col.getOriginalDatabase());
        map.put("colOrigSchema", col.getOriginalSchema());
        map.put("colOrigTable", col.getOriginalTable());
        map.put("colOrigColumn", col.getOriginalColumn());
        // Table信息
        map.put("tblDatabase", tbl.getDatabase());
        map.put("tblSchema", tbl.getSchema());
        map.put("tblName", tbl.getTable());
        return map;
    }

    // 辅助方法：将ToEdge转为Map
    private Map<String, Object> toEdgeToMap(ToEdge edge) {
        ColumnNode src = edge.getFrom();
        ColumnNode dst = edge.getTo();

        Map<String, Object> map = new HashMap<>();
        // 源Column信息
        map.put("srcDatabase", src.getDatabase());
        map.put("srcSchema", src.getSchema());
        map.put("srcTable", src.getTable());
        map.put("srcName", src.getColumn());
        // 目标Column信息
        map.put("dstDatabase", dst.getDatabase());
        map.put("dstSchema", dst.getSchema());
        map.put("dstTable", dst.getTable());
        map.put("dstName", dst.getColumn());
        return map;
    }
}
