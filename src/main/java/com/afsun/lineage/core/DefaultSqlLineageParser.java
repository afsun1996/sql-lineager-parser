package com.afsun.lineage.core;

import com.afsun.lineage.core.exceptions.InternalParseException;
import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.core.meta.DynamicMetadataProvider;
import com.afsun.lineage.core.meta.MetadataProvider;
import com.afsun.lineage.core.parser.DefaultSqlStatementHandler;
import com.afsun.lineage.core.parser.SqlStatementHandler;
import com.afsun.lineage.core.util.ClickHouseSqlRewriter;
import com.afsun.lineage.core.util.SqlDialectDetector;
import com.afsun.lineage.core.util.SqlPlainNormalizationUtil;
import com.afsun.lineage.core.util.SqlScriptUtils;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultSqlLineageParser implements SqlLineageParser {

    private final SqlStatementHandler sqlStatementHandler = new DefaultSqlStatementHandler();

    private DynamicMetadataProvider dynamicMetadataProvider;

    /**
     * 解析SQL文本，提取表和列级血缘关系
     *
     * @param sqlText SQL脚本文本
     * @param metadataProvider 元数据提供者
     * @return 解析结果，包含血缘图、警告信息等
     * @throws MetadataNotFoundException 元数据缺失异常
     * @throws UnsupportedSyntaxException 不支持的SQL语法异常
     * @throws InternalParseException 内部解析异常
     */
    @Override
    public ParseResult parse(String sqlText,DbType dbType, MetadataProvider metadataProvider) {
        long startTime = System.currentTimeMillis();
        String traceId = "LN-" + System.currentTimeMillis();
        List<LineageWarning> warns = new ArrayList<>();
        LineageGraph graph = new LineageGraph();
        AtomicInteger skipped = new AtomicInteger(0);
        // 包装为动态元数据管理器（支持脚本内临时表）
        dynamicMetadataProvider = new DynamicMetadataProvider(metadataProvider);
        try {
            // 1. 规范化SQL文本
            String formatSql = SqlPlainNormalizationUtil.process(sqlText);
            // 2. 移除注释
            String cleanedSql = SqlScriptUtils.stripComments(formatSql);
            // 3. 按分号切分多条语句
            List<String> statements = SqlScriptUtils.splitStatements(cleanedSql);
            if (dbType == null) {
                //  测方言类型
                dbType = SqlDialectDetector.detect(statements.get(0));
                log.debug("解析语句，检测到方言: {}", dbType);
            }
            // 4. ClickHouse SQL重写（将 INSERT INTO ... WITH ... SELECT 转换为标准语法）
            if (dbType == DbType.clickhouse) {
                List<String> rewrittenStatements = new ArrayList<>();
                for (String stmt : statements) {
                    rewrittenStatements.add(ClickHouseSqlRewriter.rewrite(stmt));
                }
                statements = rewrittenStatements;
            }
            if(!sqlStatementHandler.supports(dbType)){
                throw new InternalParseException("数据库类型"+dbType.toString()+",暂不支持解析");
            }
            // 5. 逐条解析语句
            for (String stmtText : statements) {
                if (stmtText.trim().isEmpty()) {
                    continue;
                }
                parseStatement(stmtText,dbType, graph, warns, skipped);
            }
            // 6. 构建成功结果
            return buildResult(traceId, startTime, graph, warns, skipped.get());
        } catch (MetadataNotFoundException | UnsupportedSyntaxException e) {
            // 业务异常：直接重新抛出，由Controller处理
            log.warn("SQL解析业务异常: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            throw e;
        } catch (InternalParseException e) {
            // 已封装的内部异常：直接抛出
            log.error("SQL解析内部异常: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            // 未预期异常：封装为InternalParseException
            log.error("SQL解析发生未预期异常, traceId={}", traceId, e);
            throw new InternalParseException(
                "脚本解析异常: " + e.getMessage() + ", traceId=" + traceId, e);
        }
    }

    /**
     * 解析单条SQL语句
     */
    private void parseStatement(String stmtText, DbType dialect,LineageGraph graph,
                                 List<LineageWarning> warns, AtomicInteger skipped) {
        try {
            // 2. 使用Druid解析SQL
            List<SQLStatement> stmts = SQLUtils.parseStatements(stmtText, dialect);
            // 3. 处理每个AST节点
            for (SQLStatement st : stmts) {
                Scope scope = new Scope(); // 每条语句独立作用域
                sqlStatementHandler.handle(st, dialect, scope, graph, dynamicMetadataProvider, warns, skipped);
            }
        } catch (MetadataNotFoundException | UnsupportedSyntaxException e) {
            // 业务异常直接抛出
            throw e;
        } catch (Exception e) {
            // 语句级异常：包装为InternalParseException并附带SQL片段信息
            throw new InternalParseException(
                "解析失败: " + shortSql(stmtText) + " -> " + e.getMessage(), e);
        }
    }

    private String shortSql(String s) {
        String t = s.replaceAll("\\s+", " ").trim();
        if (t.length() <= 200) return t;
        return t.substring(0, 100) + " ... " + t.substring(t.length() - 100);
    }


    /**
     * 构建解析结果对象
     */
    private ParseResult buildResult(String traceId, long startTime,
                                     LineageGraph graph, List<LineageWarning> warns, int skipped) {
        long elapsed = System.currentTimeMillis() - startTime;

        ParseResult result = new ParseResult();
        result.setTraceId(traceId);
        result.setGraph(graph);
        result.setWarnings(warns);
        result.setSkippedFragments(skipped);
        result.setParseMillis(elapsed);

        log.info("SQL解析完成, traceId={}, 表节点={}, 列节点={}, 警告={}, 跳过={}, 耗时={}ms",
            traceId, graph.getTables().size(), graph.getColumns().size(),
            warns.size(), skipped, elapsed);

        return result;
    }



}