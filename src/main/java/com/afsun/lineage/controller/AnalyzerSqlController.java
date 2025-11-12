package com.afsun.lineage.controller;


import com.afsun.lineage.core.ParseResult;
import com.afsun.lineage.core.exceptions.InternalParseException;
import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.core.meta.ClickHouseMetadataProvider;
import com.afsun.lineage.service.SqlLineageParseService;
import com.afsun.lineage.vo.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * SQL血缘分析API控制器
 *
 * @author afsun
 * @date 2025-11-04日 8:55
 */
@RestController
@RequestMapping("/sql/analyzer")
@Slf4j
public class AnalyzerSqlController {

    @Resource
    private SqlLineageParseService sqlLineageParseService;

    @Resource
    private ClickHouseMetadataProvider metadataProvider;

    /**
     * 文件大小限制（字节），默认10MB
     */
    @Value("${sql.lineage.max-file-size:10485760}")
    private long maxFileSize;

    /**
     * 通过上传文件解析SQL血缘关系
     *
     * @param file SQL脚本文件（UTF-8编码）
     * @return 解析结果，包含血缘图和警告信息
     */
    @PostMapping("/upload")
    public Response<ParseResult> parseFile(@RequestParam("file") MultipartFile file) {
        // 1. 参数校验
        if (file == null || file.isEmpty()) {
            return Response.fail("文件不能为空");
        }

        // 2. 文件大小检查
        if (file.getSize() > maxFileSize) {
            return Response.fail(String.format("文件大小超过限制：%.2fMB > %.2fMB",
                file.getSize() / 1024.0 / 1024.0, maxFileSize / 1024.0 / 1024.0));
        }

        // 3. 文件名校验
        String filename = file.getOriginalFilename();
        if (filename == null || filename.trim().isEmpty()) {
            return Response.fail("文件名无效");
        }

        log.info("开始解析SQL文件: {}, 大小: {} bytes", filename, file.getSize());

        try {
            // 4. 使用UTF-8编码读取文件内容
            String content = new String(file.getBytes(), StandardCharsets.UTF_8);

            // 5. 调用解析服务
            ParseResult result = sqlLineageParseService.parse(content,null);

            log.info("SQL文件解析成功: {}, traceId: {}, 耗时: {}ms",
                filename, result.getTraceId(), result.getParseMillis());

            return Response.success(result);

        } catch (MetadataNotFoundException e) {
            log.warn("元数据未找到: {}", e.getMessage());
            return Response.fail(400, "元数据缺失: " + e.getMessage());

        } catch (UnsupportedSyntaxException e) {
            log.warn("不支持的SQL语法: {}", e.getMessage());
            return Response.fail(422, "不支持的SQL语法: " + e.getMessage());

        } catch (InternalParseException e) {
            log.error("SQL解析内部错误", e);
            return Response.fail(500, "解析失败: " + e.getMessage());

        } catch (Exception e) {
            log.error("文件读取或解析异常", e);
            return Response.fail("系统错误: " + e.getMessage());
        }
    }

    /**
     * 直接解析SQL文本
     *
     * @param sqlText SQL脚本文本
     * @return 解析结果，包含血缘图和警告信息
     */
    @PostMapping("/parse")
    public Response<ParseResult> parseText(@RequestBody String sqlText) {
        // 1. 参数校验
        if (sqlText == null || sqlText.trim().isEmpty()) {
            return Response.fail("SQL文本不能为空");
        }

        // 2. 长度检查
        if (sqlText.length() > maxFileSize) {
            return Response.fail(String.format("SQL文本长度超过限制：%d > %d",
                sqlText.length(), maxFileSize));
        }

        log.info("开始解析SQL文本，长度: {} 字符", sqlText.length());

        try {
            // 3. 调用解析服务
            ParseResult result = sqlLineageParseService.parse(sqlText,null);

            log.info("SQL文本解析成功, traceId: {}, 耗时: {}ms",
                result.getTraceId(), result.getParseMillis());

            return Response.success(result);

        } catch (MetadataNotFoundException e) {
            log.warn("元数据未找到: {}", e.getMessage());
            return Response.fail(400, "元数据缺失: " + e.getMessage());

        } catch (UnsupportedSyntaxException e) {
            log.warn("不支持的SQL语法: {}", e.getMessage());
            return Response.fail(422, "不支持的SQL语法: " + e.getMessage());

        } catch (InternalParseException e) {
            log.error("SQL解析内部错误", e);
            return Response.fail(500, "解析失败: " + e.getMessage());

        } catch (Exception e) {
            log.error("SQL解析异常", e);
            return Response.fail("系统错误: " + e.getMessage());
        }
    }

    /**
     * 手动刷新元数据缓存
     *
     * @return 刷新结果统计信息
     */
    @PostMapping("/metadata/reload")
    public Response<Map<String, Object>> reloadMetadata() {
        log.info("收到手动刷新元数据请求");
        try {
            Map<String, Object> stats = metadataProvider.reload();
            if (Boolean.TRUE.equals(stats.get("success"))) {
                return Response.success(stats);
            } else {
                return Response.fail(500, "元数据刷新失败: " + stats.get("error"));
            }
        } catch (Exception e) {
            log.error("元数据刷新异常", e);
            return Response.fail("刷新失败: " + e.getMessage());
        }
    }

    /**
     * 获取元数据统计信息
     *
     * @return 统计信息（表数、列数、最后刷新时间等）
     */
    @GetMapping("/metadata/stats")
    public Response<Map<String, Object>> getMetadataStats() {
        try {
            Map<String, Object> stats = metadataProvider.getStatistics();
            return Response.success(stats);
        } catch (Exception e) {
            log.error("获取元数据统计信息异常", e);
            return Response.fail("获取统计信息失败: " + e.getMessage());
        }
    }

}
