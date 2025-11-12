package com.afsun.lineage.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 血缘查询结果
 *
 * @author afsun
 */
@Data
public class LineageQueryResult {

    /**
     * 查询类型：TABLE_UPSTREAM, TABLE_DOWNSTREAM, COLUMN_UPSTREAM, COLUMN_DOWNSTREAM
     */
    private String queryType;

    /**
     * 源对象（表或列）
     */
    private String source;

    /**
     * 查询深度
     */
    private int depth;

    /**
     * 表级血缘节点列表
     */
    private List<TableLineageNode> tableNodes = new ArrayList<>();

    /**
     * 列级血缘节点列表
     */
    private List<ColumnLineageNode> columnNodes = new ArrayList<>();

    /**
     * 血缘关系边列表
     */
    private List<LineageEdge> edges = new ArrayList<>();

    /**
     * 查询耗时（毫秒）
     */
    private long queryMillis;

    /**
     * 表级血缘节点
     */
    @Data
    public static class TableLineageNode {
        private String database;
        private String schema;
        private String tableName;
        private int level; // 层级：0表示源表，1表示直接依赖，2表示间接依赖
    }

    /**
     * 列级血缘节点
     */
    @Data
    public static class ColumnLineageNode {
        private String database;
        private String schema;
        private String tableName;
        private String columnName;
        private int level;
    }

    /**
     * 血缘关系边
     */
    @Data
    public static class LineageEdge {
        private String sourceNode; // 格式：database.schema.table 或 database.schema.table.column
        private String targetNode;
        private String edgeType; // OWNER（列属于表）或 TO（列到列的血缘）
    }
}
