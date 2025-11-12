package com.afsun.lineage.core.parser;

import com.afsun.lineage.core.LineageGraph;
import com.afsun.lineage.core.LineageWarning;
import com.afsun.lineage.core.Scope;
import com.afsun.lineage.core.meta.DynamicMetadataProvider;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SQL语句处理器接口
 * 定义处理不同类型SQL语句的统一接口
 *
 * @author afsun
 */
public interface SqlStatementHandler {

    /**
     * 判断是否支持处理该类型的SQL语句
     *
     * @param statement SQL语句AST节点
     * @return 是否支持
     */
    boolean supports(SQLStatement statement);

    /**
     * 处理SQL语句，提取血缘关系
     *
     * @param statement SQL语句AST节点
     * @param dialect SQL方言类型
     * @param scope 作用域
     * @param graph 血缘图
     * @param metadata 元数据提供者
     * @param warnings 警告列表
     * @param skipped 跳过计数器
     */
    void handle(SQLStatement statement,
                DbType dialect,
                Scope scope,
                LineageGraph graph,
                DynamicMetadataProvider metadata,
                List<LineageWarning> warnings,
                AtomicInteger skipped);
}
