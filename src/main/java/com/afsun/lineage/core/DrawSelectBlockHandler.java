package com.afsun.lineage.core;

import com.afsun.lineage.core.dto.SelectOutput;
import com.afsun.lineage.core.dto.TargetContext;
import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.core.meta.DynamicMetadataProvider;
import com.afsun.lineage.core.meta.MetadataProvider;
import com.afsun.lineage.graph.ColumnNode;
import com.afsun.lineage.graph.TableNode;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @author afsun
 * @date 2025-11-17日 16:35
 */
@Slf4j
public class DrawSelectBlockHandler {

    private final ExpressionResolver exprResolver = new DefaultExpressionResolver();

    private DynamicMetadataProvider dynamicMetadataProvider;

    private void extractSelectBlock(SQLSelectQueryBlock qb,
                                    TargetContext target,
                                    Scope scope,
                                    LineageGraph graph,
                                    DynamicMetadataProvider metadata,
                                    List<LineageWarning> warns) {
        // 1) FROM 解析：填充作用域中的表别名映射
        bindFrom(qb.getFrom(), scope, warns);
        // 2) 遍历 SELECT 列清单
        List<SelectOutput> outputs = new ArrayList<>();
        List<SQLSelectItem> items = qb.getSelectList();
        int idx = 0;
        for (SQLSelectItem item : items) {
            String alias = safeLower(item.getAlias()); // 输出列别名
            SQLExpr expr = item.getExpr();
            // 2.1 星号展开（* 或 t.*）
            if (expr instanceof SQLAllColumnExpr) {
                SQLAllColumnExpr expr2 = (SQLAllColumnExpr) expr;
                SQLExpr owner = expr2.getOwner();
                String qualifier = null;
                if(owner != null) {
                    SQLIdentifierExpr owner1 = (SQLIdentifierExpr)owner;
                    qualifier = owner1.getName();
                }
                outputs.addAll(expandStar(qualifier, scope, metadata, graph, warns));
            }  else if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlExpr = (SQLIdentifierExpr) expr;
                // ===== 修复：先判断是否为星号展开 =====
                String identName = sqlExpr.getName();
                if ("*".equals(identName)) {
                    outputs.addAll(expandStar(null, scope, metadata, graph, warns));
                } else {
                    // 普通标识符列：解析为列引用
                    List<ColumnRef> sources = exprResolver.resolve(expr, scope, metadata, warns);
                    String outName = alias != null ? alias : safeLower(identName);
                    outputs.add(new SelectOutput(outName, sources));
                }
            }
            else if (expr instanceof SQLPropertyExpr && "*".equals(((SQLPropertyExpr) expr).getName())) {
                String qualifier = ((SQLPropertyExpr) expr).getOwner().toString();
                outputs.addAll(expandStar(qualifier, scope, metadata, graph, warns));
            } else {
                // 2.2 普通表达式：解析上游源列
                List<ColumnRef> sources = exprResolver.resolve(expr, scope, metadata, warns);
                // 推断输出列名：优先别名；否则尝试从标识/属性表达式获取名称；实在没有则生成 col_{idx}
                String outName = alias;
                if (outName == null) {
                    if (expr instanceof SQLPropertyExpr) {
                        outName = safeLower(((SQLPropertyExpr) expr).getName());
                    } else if (expr instanceof SQLIdentifierExpr) {
                        outName = safeLower(((SQLIdentifierExpr) expr).getName());
                    } else {
                        outName = "col_" + idx;
                        warns.add(LineageWarning.of("ANON_COL",
                                "无法推断列名，已生成占位名: " + outName,
                                positionOf(expr), "建议为该列提供别名"));
                    }
                }
                outputs.add(new SelectOutput(outName, sources));
            }
            idx++;
        }

        // ===== 新增：解析 WHERE 子句中的列引用 =====
        if (qb.getWhere() != null) {
            exprResolver.resolve(qb.getWhere(), scope, metadata, warns);
        }

        // ===== 新增：解析 GROUP BY 子句中的列引用 =====
        if (qb.getGroupBy() != null) {
            for (SQLExpr groupExpr : qb.getGroupBy().getItems()) {
                exprResolver.resolve(groupExpr, scope, metadata, warns);
            }
        }

        // ===== 新增：解析 HAVING 子句中的列引用 =====
        if (qb.getGroupBy() != null && qb.getGroupBy().getHaving() != null) {
            exprResolver.resolve(qb.getGroupBy().getHaving(), scope, metadata, warns);
        }

        // ===== 新增：解析 ORDER BY 子句中的列引用 =====
        if (qb.getOrderBy() != null && qb.getOrderBy().getItems() != null) {
            for (SQLSelectOrderByItem orderItem : qb.getOrderBy().getItems()) {
                exprResolver.resolve(orderItem.getExpr(), scope, metadata, warns);
            }
        }

        // 3) 若存在目标上下文（CTAS/VIEW/INSERT SELECT），将输出列映射为目标表列并建立血缘
        if (target != null && target.getTargetTable() != null) {
            // 3.1 决定目标列名列表：优先 target.getColNames() 显式列清单；否则用当前输出列名
            List<String> targetCols = target.getColNames() != null && !target.getColNames().isEmpty()
                    ? target.getColNames()
                    : map(outputs, SelectOutput::getOutputName);

            // 3.2 对齐长度：按最短长度对齐，多余列发 WARN
            int n = Math.min(outputs.size(), targetCols.size());
            if (outputs.size() != targetCols.size()) {
                warns.add(LineageWarning.of("TARGET_MISMATCH",
                        "目标列数与 SELECT 输出列数不一致，按最短长度对齐",
                        positionOf(qb), "建议显式指定 INSERT/CTAS 列清单并与 SELECT 对齐"));
            }

            for (int i = 0; i < n; i++) {
                String tgtCol = safeLower(targetCols.get(i));
                SelectOutput out = outputs.get(i);
                // 3.3 创建目标列节点与 owner 关系
                ColumnNode targetColumn = new ColumnNode(
                        target.getTargetTable().getDatabase(),
                        target.getTargetTable().getSchema(),
                        target.getTargetTable().getTable(),
                        tgtCol,
                        target.getTargetTable().getOriginalDatabase(),
                        target.getTargetTable().getOriginalSchema(),
                        target.getTargetTable().getOriginalTable(),
                        tgtCol // 原始列名这里同小写；生产可保留原名
                );
                graph.addOwner(targetColumn, target.getTargetTable());
                // 3.4 为每个源列建立 to(target → source)
                for (ColumnRef src : out.getSources()) {
                    ColumnNode srcNode = toColumnNode(src);
                    // 源列 owner（列→表）
                    graph.addOwner(srcNode, toTableNode(src));
                    // 列级血缘
                    graph.addTo(targetColumn, srcNode);
                }
            }
        } else {
            // 无目标上下文：仅为源列建立 owner（不创建 to）
            for (SelectOutput out : outputs) {
                for (ColumnRef src : out.getSources()) {
                    ColumnNode srcNode = toColumnNode(src);
                    graph.addOwner(srcNode, toTableNode(src));
                }
            }
        }
    }

    // 与 extractSelectBlock 类似，但不写图，仅返回输出列描述
    private List<SelectOutput> collectSelectBlockOutputs(SQLSelectQueryBlock qb,
                                                         Scope scope,
                                                         DynamicMetadataProvider metadata,
                                                         List<LineageWarning> warns) {
        bindFrom(qb.getFrom(), scope, warns);
        List<SelectOutput> outputs = new ArrayList<>();
        int idx = 0;
        for (SQLSelectItem item : qb.getSelectList()) {
            SQLExpr expr = item.getExpr();
            String alias = safeLower(item.getAlias());
            if (expr instanceof SQLAllColumnExpr) {

                outputs.addAll(expandStar(null, scope, metadata, null, warns));
            } else if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlExpr = (SQLIdentifierExpr) expr;
                // ===== 修复：先判断是否为星号展开 =====
                String identName = sqlExpr.getName();
                if ("*".equals(identName)) {
                    outputs.addAll(expandStar(null, scope, metadata, null, warns));
                } else {
                    // 普通标识符列：解析为列引用
                    List<ColumnRef> sources = exprResolver.resolve(expr, scope, metadata, warns);
                    String outName = alias != null ? alias : safeLower(identName);
                    outputs.add(new SelectOutput(outName, sources));
                }
            } else if (expr instanceof SQLPropertyExpr && "*".equals(((SQLPropertyExpr) expr).getName())) {
                String qualifier = ((SQLPropertyExpr) expr).getOwner().toString();
                outputs.addAll(expandStar(qualifier, scope, metadata, null, warns));
            } else {
                List<ColumnRef> sources = exprResolver.resolve(expr, scope, metadata, warns);
                String outName = alias;
                if (outName == null) {
                    if (expr instanceof SQLPropertyExpr) {
                        outName = safeLower(((SQLPropertyExpr) expr).getName());
                    } else if (expr instanceof SQLIdentifierExpr) {
                        outName = safeLower(((SQLIdentifierExpr) expr).getName());
                    } else {
                        outName = "col_" + idx;
                    }
                }
                outputs.add(new SelectOutput(outName, sources));
            }
            idx++;
        }
        return outputs;
    }

    // 收集任意 SELECT（块或 UNION）输出，便于 UNION 合并
    private List<SelectOutput> collectSelectOutputs(SQLSelectQuery q,
                                                    Scope scope,
                                                    DynamicMetadataProvider metadata,
                                                    LineageGraph graph,
                                                    List<LineageWarning> warns) {
        if (q instanceof SQLSelectQueryBlock) {
            // 临时收集：不写图（target=null），仅收集 outputs
            return collectSelectBlockOutputs((SQLSelectQueryBlock) q, scope, metadata, warns);
        } else if (q instanceof SQLUnionQuery) {
            List<SelectOutput> left = collectSelectOutputs(((SQLUnionQuery) q).getLeft(), scope, metadata, graph, warns);
            List<SelectOutput> right = collectSelectOutputs(((SQLUnionQuery) q).getRight(), scope, metadata, graph, warns);
            int n = Math.min(left.size(), right.size());
            List<SelectOutput> merged = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String name = left.get(i).getOutputName() != null ? left.get(i).getOutputName() : right.get(i).getOutputName();
                Set<ColumnRef> unionSources = new LinkedHashSet<>();
                unionSources.addAll(left.get(i).getSources());
                unionSources.addAll(right.get(i).getSources());
                merged.add(new SelectOutput(name != null ? name : "col_" + i, new ArrayList<>(unionSources)));
            }
            return merged;
        }
        return Collections.emptyList();
    }


    // 绑定 FROM 表与别名到作用域
    private void bindFrom(SQLTableSource from, Scope scope, List<LineageWarning> warns) {
        if (from == null) return;
        if (from instanceof SQLExprTableSource) {
            SQLExprTableSource ts = (SQLExprTableSource) from;
            String alias = safeLower(ts.getAlias());
            String full = ts.getExpr().toString();
            TableName tn = TableName.parse(full);
            scope.addTableAlias(alias != null ? alias : tn.getTable(), tn);
            return;
        }
        if (from instanceof SQLJoinTableSource) {
            SQLJoinTableSource join = (SQLJoinTableSource) from;
            bindFrom(join.getLeft(), scope, warns);
            bindFrom(join.getRight(), scope, warns);
            return;
        }
        // ===== 修复核心:完整处理子查询 =====
        if (from instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subTS = (SQLSubqueryTableSource) from;
            String subAlias = safeLower(subTS.getAlias());

            // 如果子查询没有别名，生成一个默认别名（用于INSERT SELECT场景）
            if (subAlias == null || subAlias.isEmpty()) {
                subAlias = "__subquery_" + System.currentTimeMillis();
                warns.add(LineageWarning.of("SUBQUERY_NO_ALIAS",
                        "子查询缺少别名，已自动生成: " + subAlias,
                        positionOf(subTS), "建议为子查询提供显式别名"));
            }

            // 1. 创建子查询专用Scope(隔离作用域)
            Scope subScope = new Scope();

            // 2. 收集子查询输出列
            List<SelectOutput> subOutputs = collectSelectOutputs(
                    subTS.getSelect().getQuery(), subScope, dynamicMetadataProvider, null, warns);

            // 3. 将子查询输出列注册为虚拟表到父Scope
            TableName virtualTable = TableName.of(null, null, subAlias, null, null, subAlias);
            scope.addTableAlias(subAlias, virtualTable);

            // 4. 注册子查询列到Scope(支持 baseData.countTotal 解析)
            for (SelectOutput out : subOutputs) {
                scope.addSubqueryColumn(subAlias, out.getOutputName(), out.getSources());
            }

            // 5. 将子查询也注册到动态元数据提供者
            List<ColumnRef> subColumns = new ArrayList<>();
            for (SelectOutput out : subOutputs) {
                ColumnRef col = ColumnRef.of(
                        null, null, subAlias, out.getOutputName(),
                        null, null, subAlias, out.getOutputName()
                );
                subColumns.add(col);
            }
            dynamicMetadataProvider.registerTempTable(null, null, subAlias, subColumns);

            return;
        }
        warns.add(LineageWarning.of("UNSUPPORTED_FROM",
                "FROM 子句结构暂不支持: " + from.getClass().getSimpleName(),
                positionOf(from), "请简化 FROM"));
    }

    // 星号展开：qualifier=null 表示 ，否则 t.
    private List<SelectOutput> expandStar(String qualifier,
                                          Scope scope,
                                          DynamicMetadataProvider metadata,
                                          LineageGraph graphOrNull,
                                          List<LineageWarning> warns) {
        List<SelectOutput> outs = new ArrayList<>();
        if (qualifier == null) {
            if (scope.tableCount() == 1) {
                TableName tn = scope.getSingle();
                List<ColumnRef> cols = metadataColumns(metadata, tn, warns);
                for (ColumnRef c : cols) {
                    outs.add(new SelectOutput(c.getColumn(), Collections.singletonList(c)));
                    if (graphOrNull != null) {
                        // 仅在需要时写 owner（在 select-only 场景 write 为 null）
                        graphOrNull.addOwner(toColumnNode(c), toTableNode(c));
                    }
                }
            } else {
                throw new MetadataNotFoundException("SELECT * 存在多表或无表，无法唯一展开");
            }
        } else {
            TableName tn = scope.resolveTable(qualifier);
            if (tn == null) {
                throw new MetadataNotFoundException("无法解析别名/表名: " + qualifier);
            }
            List<ColumnRef> cols = metadataColumns(metadata, tn, warns);
            for (ColumnRef c : cols) {
                outs.add(new SelectOutput(c.getColumn(), Collections.singletonList(c)));
                if (graphOrNull != null) {
                    graphOrNull.addOwner(toColumnNode(c), toTableNode(c));
                }
            }
        }
        return outs;
    }

//    // CTAS 处理：目标表 = create ... as (select)
//    private void handleCTAS(SQLCreateTableStatement ct,
//                            Scope scope,
//                            LineageGraph graph,
//                            MetadataProvider metadata,
//                            List<LineageWarning> warns) {
//        String tableFull = ct.getTableSource().toString();
//        TableName tn = TableName.parse(tableFull);
//        TableNode targetTable = new TableNode(tn.getDb(), tn.getSc(), tn.getTable(), tn.getOdb(), tn.getOsc(), tn.getOtb());
//        TargetContext target = TargetContext.of(targetTable, Collections.emptyList());
//        handleSelect(ct.getSelect(), target, scope, graph, metadata, warns);
//    }

    private void handleCTAS(SQLCreateTableStatement ct,
                            Scope scope,
                            LineageGraph graph,
                            DynamicMetadataProvider metadata, // ===== 改为动态 =====
                            List<LineageWarning> warns) {
        String tableFull = ct.getTableSource().toString();
        TableName tn = TableName.parse(tableFull);
        TableNode targetTable = new TableNode(tn.getDb(), tn.getSc(), tn.getTable(), tn.getOdb(), tn.getOsc(), tn.getOtb());

        // ===== 先收集SELECT输出列 =====
        List<SelectOutput> outputs = collectSelectOutputs(
                ct.getSelect().getQuery(), scope, metadata, graph, warns);

        // ===== 注册CTAS表的列到元数据 =====
        List<ColumnRef> ctasColumns = new ArrayList<>();
        for (SelectOutput out : outputs) {
            ColumnRef col = ColumnRef.of(
                    tn.getDb(), tn.getSc(), tn.getTable(), out.getOutputName(),
                    tn.getOdb(), tn.getOsc(), tn.getOtb(), out.getOutputName()
            );
            ctasColumns.add(col);
        }
        metadata.registerTempTable(tn.getDb(), tn.getSc(), tn.getTable(), ctasColumns);

        // ===== 建立血缘 =====
        TargetContext target = TargetContext.of(targetTable,
                map(outputs, SelectOutput::getOutputName));
        handleSelect(ct.getSelect(), target, scope, graph, metadata, warns);
    }

    // SELECT 主入口（可被单独 SELECT、CTAS/VIEW/INSERT...SELECT 调用）
    // target 可为空：仅扫描上游源列，建立 owner；非空：映射输出列到目标列并建立 to
    private void handleSelect(SQLSelect select,
                              TargetContext target,
                              Scope scope,
                              LineageGraph graph,
                              DynamicMetadataProvider metadata,
                              List<LineageWarning> warns) {
        if (select == null || select.getQuery() == null) return;
        if (select.getQuery() instanceof SQLSelectQueryBlock) {
            extractSelectBlock((SQLSelectQueryBlock) select.getQuery(), target, scope, graph, metadata, warns);
        } else if (select.getQuery() instanceof SQLUnionQuery) {
            extractUnion((SQLUnionQuery) select.getQuery(), target, scope, graph, metadata, warns);
        } else {
            warns.add(LineageWarning.of("UNSUPPORTED_SYNTAX",
                    "不支持的 SELECT 结构: " + select.getQuery().getClass().getSimpleName(),
                    positionOf(select.getQuery()), "请展开子查询或移除复杂 UNION 结构"));
            throw new UnsupportedSyntaxException("不支持的 SELECT 结构");
        }
    }

    // CREATE VIEW v AS SELECT ...
    private void handleCreateView(SQLCreateViewStatement v,
                                  Scope scope,
                                  LineageGraph graph,
                                  DynamicMetadataProvider metadata,
                                  List<LineageWarning> warns) {
        String name = v.getName().toString();
        TableName tn = TableName.parse(name);
        // 兼容不同 Druid 版本/方言的列清单类型
        List cols = extractViewColumnNames(v);

        TableNode targetTable = new TableNode(tn.getDb(), tn.getSc(), tn.getTable(), tn.getOdb(), tn.getOsc(), tn.getOtb());
        TargetContext target = TargetContext.of(targetTable, cols);
        handleSelect(v.getSubQuery(), target, scope, graph, metadata, warns);
    }

    // INSERT INTO t(col1, col2) SELECT ...
    private void handleInsert(SQLInsertStatement ins,
                              Scope scope,
                              LineageGraph graph,
                              DynamicMetadataProvider metadata,
                              List<LineageWarning> warns) {
        String tableFull = ins.getTableName().toString();
        TableName tn = TableName.parse(tableFull);
        TableNode targetTable = new TableNode(tn.getDb(), tn.getSc(), tn.getTable(), tn.getOdb(), tn.getOsc(), tn.getOtb());
        // 收集 INSERT 显式列清单
        List<String> tgtCols = new ArrayList<>();
        if (ins.getColumns() != null && !ins.getColumns().isEmpty()) {
            for (SQLExpr c : ins.getColumns()) {
                if (c instanceof SQLIdentifierExpr) {
                    tgtCols.add(safeLower(((SQLIdentifierExpr) c).getName()));
                } else if (c instanceof SQLPropertyExpr) {
                    tgtCols.add(safeLower(((SQLPropertyExpr) c).getName()));
                }
            }
        } else {
            // 未指定列清单：回退为元数据列顺序（可能与 SELECT 输出不一致，发 WARN 并按短对齐）
            List<ColumnRef> cols = metadataColumns(metadata, tn, warns);
            for (ColumnRef c : cols) tgtCols.add(c.getColumn());
        }
        TargetContext target = TargetContext.of(targetTable, tgtCols);
        if (ins.getQuery() == null) {
            // 单纯 VALUES 不做列级血缘
            warns.add(LineageWarning.of("NO_LINEAGE_FOR_VALUES",
                    "INSERT VALUES 不解析列级血缘", positionOf(ins), "如需列级血缘请改为 INSERT ... SELECT"));
            return;
        }
        // ===== 处理 INSERT ... WITH ... SELECT 中的 WITH 子句 =====
        SQLWithSubqueryClause withClause = ins.getQuery().getWithSubQuery();
        if (withClause != null) {
            processWith(withClause, scope, graph, metadata, warns);
        }
        handleSelect(ins.getQuery(), target, scope, graph, metadata, warns);
    }

    // UPDATE a SET a.col = expr FROM ...
    private void handleUpdate(SQLUpdateStatement upd,
                              Scope scope,
                              LineageGraph graph,
                              DynamicMetadataProvider metadata,
                              List<LineageWarning> warns) {
        // 绑定主表与 FROM 表
        bindFrom(upd.getTableSource(), scope, warns);
        if (upd.getFrom() != null) bindFrom(upd.getFrom(), scope, warns);

        for (SQLUpdateSetItem it : upd.getItems()) {
            SQLExpr left = it.getColumn();
            if (!(left instanceof SQLPropertyExpr)) {
                warns.add(LineageWarning.of("UNSUPPORTED_SET",
                        "SET 左侧列非限定列，已跳过", positionOf(left), "请使用 a.col 形式"));
                continue;
            }
            SQLPropertyExpr lpe = (SQLPropertyExpr) left;
            String lAlias = safeLower(lpe.getOwner().toString());
            String lCol = safeLower(lpe.getName());
            TableName ltn = scope.resolveTable(lAlias);
            if (ltn == null) {
                throw new MetadataNotFoundException("无法解析 UPDATE 目标别名: " + lAlias);
            }
            ColumnRef targetRef = ColumnRef.of(ltn.getDb(), ltn.getSc(), ltn.getTable(), lCol, ltn.getOdb(), ltn.getOsc(), ltn.getOtb(), lCol);
            ColumnNode targetNode = toColumnNode(targetRef);
            // 右侧表达式依赖列
            List<ColumnRef> sources = exprResolver.resolve(it.getValue(), scope, metadata, warns);

            // 建立 owner/血缘
            graph.addOwner(targetNode, toTableNode(targetRef));
            for (ColumnRef src : sources) {
                ColumnNode srcNode = toColumnNode(src);
                graph.addOwner(srcNode, toTableNode(src));
                graph.addTo(targetNode, srcNode);
            }
        }
    }

    // ===== 新增:WITH子句处理 =====
    private void processWith(SQLWithSubqueryClause with, Scope scope, LineageGraph graph,
                             DynamicMetadataProvider metadata, List<LineageWarning> warns) {
        if (with == null || with.getEntries() == null) return;

        for (SQLWithSubqueryClause.Entry entry : with.getEntries()) {
            String cteName = safeLower(entry.getAlias());
            if (cteName == null) continue;

            log.debug("处理CTE: {}", cteName);

            // ===== 修复：使用独立的scope处理CTE，确保源表被添加到图中 =====
            Scope cteScope = new Scope();

            // ===== 关键修复：先处理CTE的SELECT，将其源表和列添加到图中 =====
            // 这里必须调用extractSelectBlock而不是handleSelect，以确保源表被正确处理
            SQLSelectQuery query = entry.getSubQuery().getQuery();

            log.debug("CTE {} 的查询类型: {}", cteName, query.getClass().getSimpleName());
            int tableCountBefore = graph.getTables().size();

            if (query instanceof SQLSelectQueryBlock) {
                extractSelectBlock((SQLSelectQueryBlock) query, null, cteScope, graph, metadata, warns);
            } else if (query instanceof SQLUnionQuery) {
                extractUnion((SQLUnionQuery) query, null, cteScope, graph, metadata, warns);
            }

            int tableCountAfter = graph.getTables().size();
            log.debug("CTE {} 处理后，表数量从 {} 增加到 {}", cteName, tableCountBefore, tableCountAfter);

            // 收集CTE的输出列（用于注册到父scope）
            // 注意：这里需要重新创建scope，因为上面的extractSelectBlock已经消费了cteScope
            Scope collectScope = new Scope();
            List<SelectOutput> cteOutputs = collectSelectOutputs(
                    entry.getSubQuery().getQuery(), collectScope, metadata, graph, warns);

            log.debug("CTE {} 输出列数: {}", cteName, cteOutputs.size());

            // 将CTE注册为虚拟表到父scope
            TableName virtualTable = TableName.of(null, null, cteName, null, null, cteName);
            scope.addTableAlias(cteName, virtualTable);

            // 注册CTE列到父scope
            for (SelectOutput out : cteOutputs) {
                scope.addSubqueryColumn(cteName, out.getOutputName(), out.getSources());
            }

            // 将CTE也注册到动态元数据提供者，以便后续查询可以获取其列信息
            List<ColumnRef> cteColumns = new ArrayList<>();
            for (SelectOutput out : cteOutputs) {
                ColumnRef col = ColumnRef.of(
                        null, null, cteName, out.getOutputName(),
                        null, null, cteName, out.getOutputName()
                );
                cteColumns.add(col);
            }
            metadata.registerTempTable(null, null, cteName, cteColumns);

            // 记录CTE文本(可选)
            scope.putCTE(cteName, entry.getSubQuery().toString());
        }
    }
    // UNION 处理：按位置对齐两侧输出，并合并其 sources 到统一输出列
    private void extractUnion(SQLUnionQuery uq,
                              TargetContext target,
                              Scope scope,
                              LineageGraph graph,
                              DynamicMetadataProvider metadata,
                              List<LineageWarning> warns) {
        // 仅处理二元 UNION；链式 UNION 可递归处理 left 或 right
        List<SelectOutput> left = collectSelectOutputs(uq.getLeft(), scope, metadata, graph, warns);
        List<SelectOutput> right = collectSelectOutputs(uq.getRight(), scope, metadata, graph, warns);
        int n = Math.min(left.size(), right.size());
        if (left.size() != right.size()) {
            warns.add(LineageWarning.of("UNION_MISMATCH",
                    "UNION 分支列数不一致，按最短长度位置对齐",
                    positionOf(uq), "建议为两侧提供一致的列数与别名"));
        }
        // 将左右分支对应列的 sources 合并为一个输出集合
        List<SelectOutput> merged = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String name = left.get(i).getOutputName() != null ? left.get(i).getOutputName() : right.get(i).getOutputName();
            Set<ColumnRef> unionSources = new LinkedHashSet<>();
            unionSources.addAll(left.get(i).getSources());
            unionSources.addAll(right.get(i).getSources());
            merged.add(new SelectOutput(name != null ? name : "col_" + i, new ArrayList<>(unionSources)));
        }

        // 若有目标上下文，则映射 merged 到目标；否则仅记录 owner
        if (target != null && target.getTargetTable() != null) {
            List<String> tgtCols = target.getColNames() != null && !target.getColNames().isEmpty()
                    ? target.getColNames()
                    : map(merged, o -> o.getOutputName());
            int m = Math.min(merged.size(), tgtCols.size());
            for (int i = 0; i < m; i++) {
                String tgtCol = safeLower(tgtCols.get(i));
                SelectOutput out = merged.get(i);
                ColumnNode targetColumn = new ColumnNode(
                        target.getTargetTable().getDatabase(),
                        target.getTargetTable().getSchema(),
                        target.getTargetTable().getTable(),
                        tgtCol,
                        target.getTargetTable().getOriginalDatabase(),
                        target.getTargetTable().getOriginalSchema(),
                        target.getTargetTable().getOriginalTable(),
                        tgtCol
                );
                graph.addOwner(targetColumn, target.getTargetTable());
                for (ColumnRef src : out.getSources()) {
                    ColumnNode srcNode = toColumnNode(src);
                    graph.addOwner(srcNode, toTableNode(src));
                    graph.addTo(targetColumn, srcNode);
                }
            }
        } else {
            for (SelectOutput out : merged) {
                for (ColumnRef src : out.getSources()) {
                    ColumnNode srcNode = toColumnNode(src);
                    graph.addOwner(srcNode, toTableNode(src));
                }
            }
        }
    }

    private List extractViewColumnNames(SQLCreateViewStatement v) {
        List cols = new ArrayList<>();
        try {
            List<?> list = v.getColumns(); // 1.2.8 可能是 List/List/方言类
            if (list != null) {
                for (Object o : list) {
                    if (o instanceof SQLName) {
                        cols.add(safeLower(((SQLName) o).getSimpleName()));
                    } else if (o instanceof SQLSelectItem) {
                        SQLSelectItem si = (SQLSelectItem) o;
                        String alias = si.getAlias();
                        if (alias != null) cols.add(safeLower(alias));
                        else cols.add(deriveNameFromExpr(si.getExpr()));
                    } else if (o instanceof SQLExpr) {
                        cols.add(deriveNameFromExpr((SQLExpr) o));
                    } else {
                        // 兼容可能的 SQLColumnDefinition 等
                        try {
                            // 若存在 getName(): SQLName
                            java.lang.reflect.Method m = o.getClass().getMethod("getName");
                            Object nameObj = m.invoke(o);
                            if (nameObj instanceof SQLName) {
                                cols.add(safeLower(((SQLName) nameObj).getSimpleName()));
                                continue;
                            }
                        } catch (Exception ignore) {
                        }
                        cols.add(safeLower(String.valueOf(o)));
                    }
                }
            }
        } catch (Throwable ignore) {
            // 出错不阻断
        }
        return cols;
    }


    private String deriveNameFromExpr(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) return safeLower(((SQLIdentifierExpr) expr).getName());
        if (expr instanceof SQLPropertyExpr) return safeLower(((SQLPropertyExpr) expr).getName());
        return safeLower(expr == null ? null : expr.toString());
    }

    // 工具：从元数据获取列清单并转 ColumnRef
    private List<ColumnRef> metadataColumns(MetadataProvider metadata, TableName tn, List<LineageWarning> warns) {
        List<ColumnRef> cols = metadata.getColumns(tn.getDb(), tn.getSc(), tn.getTable());
        if (cols == null || cols.isEmpty()) {
            throw new MetadataNotFoundException("元数据缺失: " + tn.toString());
        }
        return cols;
    }


    private String positionOf(Object ast) {
        // 占位：可用 Druid 的 parser position 获取位置信息，这里返回简单类型名
        return ast.getClass().getSimpleName();
    }


    private String safeLower(String s) {
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }

    private <T, R> List<R> map(List<T> list, java.util.function.Function<T, R> f) {
        List<R> r = new ArrayList<>();
        for (T t : list) r.add(f.apply(t));
        return r;
    }

    // ColumnRef/表名到图节点的简单映射（小写键 + original*）
    private ColumnNode toColumnNode(ColumnRef c) {
        return new ColumnNode(c.getDatabase(), c.getSchema(), c.getTable(), c.getColumn(),
                c.getOriginalDatabase(), c.getOriginalSchema(), c.getOriginalTable(), c.getOriginalColumn());
    }

    private TableNode toTableNode(ColumnRef c) {
        return new TableNode(c.getDatabase(), c.getSchema(), c.getTable(),
                c.getOriginalDatabase(), c.getOriginalSchema(), c.getOriginalTable());
    }

}
