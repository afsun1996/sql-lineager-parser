package com.afsun.lineage.controller;

import com.afsun.lineage.service.LineageQueryService;
import com.afsun.lineage.vo.LineageQueryResult;
import com.afsun.lineage.vo.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * 血缘查询API控制器
 * 提供表级和列级血缘关系的查询接口
 *
 * @author afsun
 */
@RestController
@RequestMapping("/sql/lineage")
@Slf4j
public class LineageQueryController {

    @Resource
    private LineageQueryService lineageQueryService;

    /**
     * 查询表的上游依赖
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param depth 查询深度，默认为1
     * @return 上游表列表
     */
    @GetMapping("/table/upstream")
    public Response<LineageQueryResult> queryUpstreamTables(
            @RequestParam(required = false) String database,
            @RequestParam String schema,
            @RequestParam String tableName,
            @RequestParam(defaultValue = "1") int depth) {

        log.info("查询表上游依赖: {}.{}.{}, depth={}", database, schema, tableName, depth);

        if (depth < 1 || depth > 10) {
            return Response.fail("查询深度必须在1-10之间");
        }

        try {
            LineageQueryResult result = lineageQueryService.queryUpstreamTables(
                    database, schema, tableName, depth);
            return Response.success(result);
        } catch (Exception e) {
            log.error("查询表上游依赖失败", e);
            return Response.fail("查询失败: " + e.getMessage());
        }
    }

    /**
     * 查询表的下游依赖
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param depth 查询深度，默认为1
     * @return 下游表列表
     */
    @GetMapping("/table/downstream")
    public Response<LineageQueryResult> queryDownstreamTables(
            @RequestParam(required = false) String database,
            @RequestParam String schema,
            @RequestParam String tableName,
            @RequestParam(defaultValue = "1") int depth) {

        log.info("查询表下游依赖: {}.{}.{}, depth={}", database, schema, tableName, depth);

        if (depth < 1 || depth > 10) {
            return Response.fail("查询深度必须在1-10之间");
        }

        try {
            LineageQueryResult result = lineageQueryService.queryDownstreamTables(
                    database, schema, tableName, depth);
            return Response.success(result);
        } catch (Exception e) {
            log.error("查询表下游依赖失败", e);
            return Response.fail("查询失败: " + e.getMessage());
        }
    }

    /**
     * 查询列的上游依赖
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param columnName 列名
     * @param depth 查询深度，默认为1
     * @return 上游列列表
     */
    @GetMapping("/column/upstream")
    public Response<LineageQueryResult> queryUpstreamColumns(
            @RequestParam(required = false) String database,
            @RequestParam String schema,
            @RequestParam String tableName,
            @RequestParam String columnName,
            @RequestParam(defaultValue = "1") int depth) {

        log.info("查询列上游依赖: {}.{}.{}.{}, depth={}", database, schema, tableName, columnName, depth);

        if (depth < 1 || depth > 10) {
            return Response.fail("查询深度必须在1-10之间");
        }

        try {
            LineageQueryResult result = lineageQueryService.queryUpstreamColumns(
                    database, schema, tableName, columnName, depth);
            return Response.success(result);
        } catch (Exception e) {
            log.error("查询列上游依赖失败", e);
            return Response.fail("查询失败: " + e.getMessage());
        }
    }

    /**
     * 查询列的下游依赖
     *
     * @param database 数据库名
     * @param schema 模式名
     * @param tableName 表名
     * @param columnName 列名
     * @param depth 查询深度，默认为1
     * @return 下游列列表
     */
    @GetMapping("/column/downstream")
    public Response<LineageQueryResult> queryDownstreamColumns(
            @RequestParam(required = false) String database,
            @RequestParam String schema,
            @RequestParam String tableName,
            @RequestParam String columnName,
            @RequestParam(defaultValue = "1") int depth) {

        log.info("查询列下游依赖: {}.{}.{}.{}, depth={}", database, schema, tableName, columnName, depth);

        if (depth < 1 || depth > 10) {
            return Response.fail("查询深度必须在1-10之间");
        }

        try {
            LineageQueryResult result = lineageQueryService.queryDownstreamColumns(
                    database, schema, tableName, columnName, depth);
            return Response.success(result);
        } catch (Exception e) {
            log.error("查询列下游依赖失败", e);
            return Response.fail("查询失败: " + e.getMessage());
        }
    }

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
    @GetMapping("/path")
    public Response<List<List<String>>> queryLineagePath(
            @RequestParam(required = false) String sourceDatabase,
            @RequestParam String sourceSchema,
            @RequestParam String sourceTable,
            @RequestParam(required = false) String targetDatabase,
            @RequestParam String targetSchema,
            @RequestParam String targetTable) {

        log.info("查询血缘路径: {}.{}.{} -> {}.{}.{}",
                sourceDatabase, sourceSchema, sourceTable,
                targetDatabase, targetSchema, targetTable);

        try {
            List<List<String>> paths = lineageQueryService.queryLineagePath(
                    sourceDatabase, sourceSchema, sourceTable,
                    targetDatabase, targetSchema, targetTable);
            return Response.success(paths);
        } catch (Exception e) {
            log.error("查询血缘路径失败", e);
            return Response.fail("查询失败: " + e.getMessage());
        }
    }
}
