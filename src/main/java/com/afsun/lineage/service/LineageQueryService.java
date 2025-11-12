package com.afsun.lineage.service;

import com.afsun.lineage.vo.LineageQueryResult;

import java.util.List;

/**
 * 血缘查询服务接口
 * 提供表级和列级血缘关系的查询功能
 *
 * @author afsun
 */
public interface LineageQueryService {

    /**
     * 查询指定表的上游依赖（该表的数据来源于哪些表）
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param depth 查询深度（1表示直接上游，2表示上游的上游，以此类推）
     * @return 上游表列表
     */
    LineageQueryResult queryUpstreamTables(String database, String schema, String tableName, int depth);

    /**
     * 查询指定表的下游依赖（该表的数据被哪些表使用）
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param depth 查询深度
     * @return 下游表列表
     */
    LineageQueryResult queryDownstreamTables(String database, String schema, String tableName, int depth);

    /**
     * 查询指定列的上游依赖（该列的数据来源于哪些列）
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param columnName 列名
     * @param depth 查询深度
     * @return 上游列列表
     */
    LineageQueryResult queryUpstreamColumns(String database, String schema, String tableName, String columnName, int depth);

    /**
     * 查询指定列的下游依赖（该列的数据被哪些列使用）
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param columnName 列名
     * @param depth 查询深度
     * @return 下游列列表
     */
    LineageQueryResult queryDownstreamColumns(String database, String schema, String tableName, String columnName, int depth);

    /**
     * 查询两个表之间的血缘路径
     *
     * @param sourceDatabase 源数据库
     * @param sourceSchema 源模式
     * @param sourceTable 源表
     * @param targetDatabase 目标数据库
     * @param targetSchema 目标模式
     * @param targetTable 目标表
     * @return 血缘路径列表
     */
    List<List<String>> queryLineagePath(String sourceDatabase, String sourceSchema, String sourceTable,
                                         String targetDatabase, String targetSchema, String targetTable);
}
