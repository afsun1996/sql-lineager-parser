package com.afsun.lineage.core;

import com.afsun.lineage.core.meta.MetadataProvider;
import com.alibaba.druid.sql.ast.SQLExpr;
import java.util.List;

/**
 * SQL表达式解析器
 *
 * @author afsun
 *
 */
public interface ExpressionResolver {
    /**
     * 解析SQL表达式
     * @param expr sql表达式
     * @param scope 上下文
     * @param metadata 元数据接口u
     * @param warns 告警提示数据
     * @return 字段列表
     */
    List<ColumnRef> resolve(SQLExpr expr, Scope scope, MetadataProvider metadata, List<LineageWarning> warns);
}