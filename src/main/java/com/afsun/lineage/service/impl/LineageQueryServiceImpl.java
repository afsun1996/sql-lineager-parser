package com.afsun.lineage.service.impl;

import com.afsun.lineage.service.LineageQueryService;
import com.afsun.lineage.vo.LineageQueryResult;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Value;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 血缘查询服务实现
 * 基于Neo4j图数据库进行血缘关系查询
 *
 * @author afsun
 */
@Service
@Slf4j
public class LineageQueryServiceImpl implements LineageQueryService {

    private final Neo4jClient neo4jClient;

    public LineageQueryServiceImpl(Neo4jClient neo4jClient) {
        this.neo4jClient = neo4jClient;
    }

    @Override
    public LineageQueryResult queryUpstreamTables(String database, String schema, String tableName, int depth) {
        long startTime = System.currentTimeMillis();

        String cypher =
            "MATCH path = (target:Table {database: $database, schema: $schema, name: $tableName})" +
            "<-[:BELONGS_TO*0.." + (depth * 2) + "]-(upstream:Table) " +
            "WHERE target <> upstream " +
            "WITH upstream, length(path) / 2 AS level " +
            "RETURN DISTINCT upstream.database AS database, upstream.schema AS schema, " +
            "upstream.name AS tableName, level " +
            "ORDER BY level, schema, tableName";

        List<LineageQueryResult.TableLineageNode> nodes = neo4jClient.query(cypher)
            .bind(database != null ? database : "UNKNOWN").to("database")
            .bind(schema).to("schema")
            .bind(tableName).to("tableName")
            .fetchAs(LineageQueryResult.TableLineageNode.class)
            .mappedBy((typeSystem, record) -> {
                LineageQueryResult.TableLineageNode node = new LineageQueryResult.TableLineageNode();
                node.setDatabase(record.get("database").asString());
                node.setSchema(record.get("schema").asString());
                node.setTableName(record.get("tableName").asString());
                node.setLevel(record.get("level").asInt());
                return node;
            })
            .all()
            .stream()
            .collect(Collectors.toList());

        LineageQueryResult result = new LineageQueryResult();
        result.setQueryType("TABLE_UPSTREAM");
        result.setSource(schema + "." + tableName);
        result.setDepth(depth);
        result.setTableNodes(nodes);
        result.setQueryMillis(System.currentTimeMillis() - startTime);

        log.info("查询表上游依赖完成: {}.{}, 找到{}个节点, 耗时{}ms",
            schema, tableName, nodes.size(), result.getQueryMillis());

        return result;
    }

    @Override
    public LineageQueryResult queryDownstreamTables(String database, String schema, String tableName, int depth) {
        long startTime = System.currentTimeMillis();

        String cypher =
            "MATCH path = (source:Table {database: $database, schema: $schema, name: $tableName})" +
            "-[:BELONGS_TO*0.." + (depth * 2) + "]->(downstream:Table) " +
            "WHERE source <> downstream " +
            "WITH downstream, length(path) / 2 AS level " +
            "RETURN DISTINCT downstream.database AS database, downstream.schema AS schema, " +
            "downstream.name AS tableName, level " +
            "ORDER BY level, schema, tableName";

        List<LineageQueryResult.TableLineageNode> nodes = neo4jClient.query(cypher)
            .bind(database != null ? database : "UNKNOWN").to("database")
            .bind(schema).to("schema")
            .bind(tableName).to("tableName")
            .fetchAs(LineageQueryResult.TableLineageNode.class)
            .mappedBy((typeSystem, record) -> {
                LineageQueryResult.TableLineageNode node = new LineageQueryResult.TableLineageNode();
                node.setDatabase(record.get("database").asString());
                node.setSchema(record.get("schema").asString());
                node.setTableName(record.get("tableName").asString());
                node.setLevel(record.get("level").asInt());
                return node;
            })
            .all()
            .stream()
            .collect(Collectors.toList());

        LineageQueryResult result = new LineageQueryResult();
        result.setQueryType("TABLE_DOWNSTREAM");
        result.setSource(schema + "." + tableName);
        result.setDepth(depth);
        result.setTableNodes(nodes);
        result.setQueryMillis(System.currentTimeMillis() - startTime);

        log.info("查询表下游依赖完成: {}.{}, 找到{}个节点, 耗时{}ms",
            schema, tableName, nodes.size(), result.getQueryMillis());

        return result;
    }

    @Override
    public LineageQueryResult queryUpstreamColumns(String database, String schema, String tableName,
                                                   String columnName, int depth) {
        long startTime = System.currentTimeMillis();

        String cypher =
            "MATCH path = (target:Column {database: $database, schema: $schema, tableName: $tableName, name: $columnName})" +
            "<-[:LINKS_TO*1.." + depth + "]-(upstream:Column) " +
            "WITH upstream, length(path) AS level " +
            "RETURN DISTINCT upstream.database AS database, upstream.schema AS schema, " +
            "upstream.tableName AS tableName, upstream.name AS columnName, level " +
            "ORDER BY level, schema, tableName, columnName";

        List<LineageQueryResult.ColumnLineageNode> nodes = new ArrayList<>(neo4jClient.query(cypher)
                .bind(database != null ? database : "UNKNOWN").to("database")
                .bind(schema).to("schema")
                .bind(tableName).to("tableName")
                .bind(columnName).to("columnName")
                .fetchAs(LineageQueryResult.ColumnLineageNode.class)
                .mappedBy((typeSystem, record) -> {
                    LineageQueryResult.ColumnLineageNode node = new LineageQueryResult.ColumnLineageNode();
                    node.setDatabase(record.get("database").asString());
                    node.setSchema(record.get("schema").asString());
                    node.setTableName(record.get("tableName").asString());
                    node.setColumnName(record.get("columnName").asString());
                    node.setLevel(record.get("level").asInt());
                    return node;
                })
                .all());

        // 查询边关系
        List<LineageQueryResult.LineageEdge> edges = queryColumnEdges(database, schema, tableName, columnName, depth, true);

        LineageQueryResult result = new LineageQueryResult();
        result.setQueryType("COLUMN_UPSTREAM");
        result.setSource(schema + "." + tableName + "." + columnName);
        result.setDepth(depth);
        result.setColumnNodes(nodes);
        result.setEdges(edges);
        result.setQueryMillis(System.currentTimeMillis() - startTime);

        log.info("查询列上游依赖完成: {}.{}.{}, 找到{}个节点, 耗时{}ms",
            schema, tableName, columnName, nodes.size(), result.getQueryMillis());

        return result;
    }

    @Override
    public LineageQueryResult queryDownstreamColumns(String database, String schema, String tableName,
                                                     String columnName, int depth) {
        long startTime = System.currentTimeMillis();

        String cypher =
            "MATCH path = (source:Column {database: $database, schema: $schema, tableName: $tableName, name: $columnName})" +
            "-[:LINKS_TO*1.." + depth + "]->(downstream:Column) " +
            "WITH downstream, length(path) AS level " +
            "RETURN DISTINCT downstream.database AS database, downstream.schema AS schema, " +
            "downstream.tableName AS tableName, downstream.name AS columnName, level " +
            "ORDER BY level, schema, tableName, columnName";

        List<LineageQueryResult.ColumnLineageNode> nodes = neo4jClient.query(cypher)
            .bind(database != null ? database : "UNKNOWN").to("database")
            .bind(schema).to("schema")
            .bind(tableName).to("tableName")
            .bind(columnName).to("columnName")
            .fetchAs(LineageQueryResult.ColumnLineageNode.class)
            .mappedBy((typeSystem, record) -> {
                LineageQueryResult.ColumnLineageNode node = new LineageQueryResult.ColumnLineageNode();
                node.setDatabase(record.get("database").asString());
                node.setSchema(record.get("schema").asString());
                node.setTableName(record.get("tableName").asString());
                node.setColumnName(record.get("columnName").asString());
                node.setLevel(record.get("level").asInt());
                return node;
            })
            .all()
            .stream()
            .collect(Collectors.toList());

        // 查询边关系
        List<LineageQueryResult.LineageEdge> edges = queryColumnEdges(database, schema, tableName, columnName, depth, false);

        LineageQueryResult result = new LineageQueryResult();
        result.setQueryType("COLUMN_DOWNSTREAM");
        result.setSource(schema + "." + tableName + "." + columnName);
        result.setDepth(depth);
        result.setColumnNodes(nodes);
        result.setEdges(edges);
        result.setQueryMillis(System.currentTimeMillis() - startTime);

        log.info("查询列下游依赖完成: {}.{}.{}, 找到{}个节点, 耗时{}ms",
            schema, tableName, columnName, nodes.size(), result.getQueryMillis());

        return result;
    }

    @Override
    public List<List<String>> queryLineagePath(String sourceDatabase, String sourceSchema, String sourceTable,
                                               String targetDatabase, String targetSchema, String targetTable) {
        long startTime = System.currentTimeMillis();

        String cypher =
            "MATCH path = (source:Table {database: $sourceDatabase, schema: $sourceSchema, name: $sourceTable})" +
            "-[:BELONGS_TO*..20]->" +
            "(target:Table {database: $targetDatabase, schema: $targetSchema, name: $targetTable}) " +
            "WHERE source <> target " +
            "WITH path, [node IN nodes(path) WHERE node:Table | node.schema + '.' + node.name] AS tablePath " +
            "RETURN tablePath " +
            "ORDER BY length(path) " +
            "LIMIT 10";

        Collection<List> pathCollection = neo4jClient.query(cypher)
            .bind(sourceDatabase != null ? sourceDatabase : "UNKNOWN").to("sourceDatabase")
            .bind(sourceSchema).to("sourceSchema")
            .bind(sourceTable).to("sourceTable")
            .bind(targetDatabase != null ? targetDatabase : "UNKNOWN").to("targetDatabase")
            .bind(targetSchema).to("targetSchema")
            .bind(targetTable).to("targetTable")
            .<List>fetchAs(List.class)
            .mappedBy((typeSystem, record) -> {
               return record.get("tablePath").asList(new Function<Value, String>() {
                    @Override
                    public String apply(Value value) {
                        return value.asString();
                    }
                });
//                return record.get("tablePath").asList(value -> value.asString());
            })
            .all();
        List<List<String>> collect = pathCollection.stream()
                .map(v -> (List<String>) v.stream().map(Object::toString).collect(Collectors.toList()))
                .collect(Collectors.toList());

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("查询血缘路径完成: {}.{} -> {}.{}, 找到{}条路径, 耗时{}ms",
            sourceSchema, sourceTable, targetSchema, targetTable, collect.size(), elapsed);

        return collect;
    }

    /**
     * 查询列级血缘边关系
     */
    private List<LineageQueryResult.LineageEdge> queryColumnEdges(String database, String schema,
                                                                  String tableName, String columnName,
                                                                  int depth, boolean upstream) {
        String direction = upstream ? "<-" : "-";
        String cypher =
            "MATCH (start:Column {database: $database, schema: $schema, tableName: $tableName, name: $columnName})" +
            direction + "[:LINKS_TO*1.." + depth + "]" + (upstream ? "-" : "->") + "(end:Column) " +
            "MATCH (start)-[r:LINKS_TO]-(end) " +
            "RETURN DISTINCT " +
            "start.schema + '.' + start.tableName + '.' + start.name AS sourceNode, " +
            "end.schema + '.' + end.tableName + '.' + end.name AS targetNode, " +
            "'TO' AS edgeType";

        return neo4jClient.query(cypher)
            .bind(database != null ? database : "UNKNOWN").to("database")
            .bind(schema).to("schema")
            .bind(tableName).to("tableName")
            .bind(columnName).to("columnName")
            .fetchAs(LineageQueryResult.LineageEdge.class)
            .mappedBy((typeSystem, record) -> {
                LineageQueryResult.LineageEdge edge = new LineageQueryResult.LineageEdge();
                edge.setSourceNode(record.get("sourceNode").asString());
                edge.setTargetNode(record.get("targetNode").asString());
                edge.setEdgeType(record.get("edgeType").asString());
                return edge;
            })
            .all()
            .stream()
            .collect(Collectors.toList());
    }
}
