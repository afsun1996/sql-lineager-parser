package com.afsun.lineage.core;

import com.afsun.lineage.core.meta.MetadataProvider;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;


public class DefaultExpressionResolver implements ExpressionResolver {

    @Override
    public List<ColumnRef> resolve(SQLExpr expr, Scope scope, MetadataProvider metadata, List<LineageWarning> warns) {
        LinkedHashSet<ColumnRef> set = new LinkedHashSet<>();
        walk(expr, scope, metadata, warns, set);
        return new ArrayList<>(set);
    }

    // 递归下探表达式树，收集所有真实列引用
    private void walk(SQLObject node, Scope scope, MetadataProvider metadata, List<LineageWarning> warns, LinkedHashSet<ColumnRef> out) {
        if (node == null) return;

        // 未限定列: col（仅单表作用域允许）
        if (node instanceof SQLIdentifierExpr) {
            String col = ((SQLIdentifierExpr) node).getName();
            ColumnRef ref = scope.resolveUnqualifiedColumn(col, warns);
            if (ref != null) out.add(ref);
            return;
        }

        // 限定列: t.col (增强:支持子查询列)
        if (node instanceof SQLPropertyExpr) {
            SQLPropertyExpr pe = (SQLPropertyExpr) node;
            String owner = pe.getOwner() == null ? null : pe.getOwner().toString();
            String name = pe.getName();

            if ("*".equals(name) || (name == null || name.isEmpty())) {
                expandStar(owner, scope, metadata, warns, out);
            } else {
                // ===== 修改:优先解析子查询列 =====
                List<ColumnRef> subSources = scope.getSubqueryColumnSources(owner, name);
                if (subSources != null && !subSources.isEmpty()) {
                    // 子查询列可能来自多个源,全部添加
                    out.addAll(subSources);
                } else {
                    // 回退到普通列解析
                    ColumnRef ref = scope.resolveQualifiedColumn(owner, name, warns);
                    if (ref != null) out.add(ref);
                }
            }
            return;
        }
        // 方法/函数调用 foo(a,b)；递归参数
        if (node instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr m = (SQLMethodInvokeExpr) node;
            for (SQLExpr arg : m.getArguments()) {
                walk(arg, scope, metadata, warns, out);
            }
            return;
        }

        // 聚合 SUM(col) / COUNT(*) [OVER (...)]
        if (node instanceof SQLAggregateExpr) {
            SQLAggregateExpr ag = (SQLAggregateExpr) node;
            for (SQLExpr arg : ag.getArguments()) {
                if (!(arg instanceof SQLAllColumnExpr)) {
                    walk(arg, scope, metadata, warns, out);
                }
            }
            SQLOver over = ag.getOver();
            if (over != null) {
                if (over.getPartitionBy() != null) {
                    for (SQLExpr p : over.getPartitionBy()) walk(p, scope, metadata, warns, out);
                }
                SQLOrderBy ob = over.getOrderBy();
                if (ob != null && ob.getItems() != null) {
                    for (SQLSelectOrderByItem i : ob.getItems()) {
                        walk(i.getExpr(), scope, metadata, warns, out);
                    }
                }
            }
            return;
        }

        // CASE WHEN
        if (node instanceof SQLCaseExpr) {
            SQLCaseExpr c = (SQLCaseExpr) node;
            for (SQLCaseExpr.Item it : c.getItems()) {
                walk(it.getConditionExpr(), scope, metadata, warns, out);
                walk(it.getValueExpr(), scope, metadata, warns, out);
            }
            walk(c.getElseExpr(), scope, metadata, warns, out);
            return;
        }

        // 二元操作: a + b / a = b
        if (node instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr b = (SQLBinaryOpExpr) node;
            walk(b.getLeft(), scope, metadata, warns, out);
            walk(b.getRight(), scope, metadata, warns, out);
            return;
        }

        // 一元操作: -a / NOT a
        if (node instanceof SQLUnaryExpr) {
            SQLUnaryExpr u = (SQLUnaryExpr) node;
            walk(u.getExpr(), scope, metadata, warns, out);
            return;
        }

        // BETWEEN a AND b
        if (node instanceof SQLBetweenExpr) {
            SQLBetweenExpr be = (SQLBetweenExpr) node;
            walk(be.getTestExpr(), scope, metadata, warns, out);
            walk(be.getBeginExpr(), scope, metadata, warns, out);
            walk(be.getEndExpr(), scope, metadata, warns, out);
            return;
        }

        // IN 列表: x IN (a, b, c)
        if (node instanceof SQLInListExpr) {
            SQLInListExpr in = (SQLInListExpr) node;
            walk(in.getExpr(), scope, metadata, warns, out);
            for (SQLExpr e : in.getTargetList()) {
                walk(e, scope, metadata, warns, out);
            }
            return;
        }

        // IN 子查询: x IN (SELECT ...)
        if (node instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr ins = (SQLInSubQueryExpr) node;
            walk(ins.getExpr(), scope, metadata, warns, out);
            if (ins.getSubQuery() != null) {
                walkSelect(ins.getSubQuery(), scope, metadata, warns, out);
            }
            return;
        }

        // EXISTS (SELECT ...)
        if (node instanceof SQLExistsExpr) {
            SQLExistsExpr ex = (SQLExistsExpr) node;
            if (ex.getSubQuery() != null) {
                walkSelect(ex.getSubQuery(), scope, metadata, warns, out);
            }
            return;
        }

        // 标量子查询: (SELECT ...)
        if (node instanceof SQLQueryExpr) {
            SQLQueryExpr qe = (SQLQueryExpr) node;
            if (qe.getSubQuery() != null) {
                walkSelect(qe.getSubQuery(), scope, metadata, warns, out);
            }
            return;
        }

        if (node instanceof SQLCastExpr) {
            walk(((SQLCastExpr) node).getExpr(), scope, metadata, warns, out);
            return;
        }

        // 数组（某些方言）
        if (node instanceof SQLArrayExpr) {
            SQLArrayExpr arrayExpr = (SQLArrayExpr) node;
            walk(arrayExpr.getExpr(), scope, metadata, warns, out);
            return;
        }
        if (node instanceof SQLCharExpr
                || node instanceof SQLIntegerExpr
                || node instanceof SQLFloatExpr
                || node instanceof SQLBooleanExpr
                || node instanceof SQLDoubleExpr) {
            return;
        }
        // 其他类型按需扩展
        warns.add(LineageWarning.of("UNSUPPORTED_Expr",
                "不支持的SELECT语法表达式: " + node.getClass().getSimpleName(),
                node.getClass().getSimpleName(),
                "新增适配算法"));
    }

    // 遍历子查询 SELECT（支持块/UNION），使用同一个 Scope 临时绑定 FROM
    private void walkSelect(SQLSelect select, Scope scope, MetadataProvider metadata, List<LineageWarning> warns, LinkedHashSet<ColumnRef> out) {
        if (select == null || select.getQuery() == null) return;

        SQLSelectQuery q = select.getQuery();

        if (q instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock qb = (SQLSelectQueryBlock) q;

            bindFrom(qb.getFrom(), scope, warns);

            // SELECT 列清单
            List<SQLSelectItem> items = qb.getSelectList();
            if (items != null) {
                for (SQLSelectItem it : items) {
                    SQLExpr expr = it.getExpr();
                    if (expr instanceof SQLAllColumnExpr) {
                        // SELECT ：单表时展开；多表发WARN
                        expandStar(null, scope, metadata, warns, out);
                    } else if (expr instanceof SQLPropertyExpr) {
                        SQLPropertyExpr pe = (SQLPropertyExpr) expr;
                        if ("".equals(pe.getName()) || (pe.getName() == null || pe.getName().isEmpty())) {
                            String qualifier = pe.getOwner() == null ? null : pe.getOwner().toString();
                            expandStar(qualifier, scope, metadata, warns, out);
                        } else {
                            walk(expr, scope, metadata, warns, out);
                        }
                    } else {
                        walk(expr, scope, metadata, warns, out);
                    }
                }
            }
        } else if (q instanceof SQLUnionQuery) {
            SQLUnionQuery u = (SQLUnionQuery) q;
            SQLSelect left = new SQLSelect();
            left.setQuery(u.getLeft());
            SQLSelect right = new SQLSelect();
            right.setQuery(u.getRight());
            walkSelect(left, scope, metadata, warns, out);
            walkSelect(right, scope, metadata, warns, out);
        } else {
            warns.add(LineageWarning.of("UNSUPPORTED_SUBQUERY",
                    "不支持的子查询结构: " + q.getClass().getSimpleName(),
                    q.getClass().getSimpleName(),
                    "请简化子查询"));
        }
    }

    // 在当前作用域绑定 FROM 表与别名（支持 JOIN），并解析 JOIN 条件中的列
    private void bindFrom(SQLTableSource from, Scope scope, List<LineageWarning> warns) {
        if (from == null) return;

        if (from instanceof SQLExprTableSource) {
            SQLExprTableSource ts = (SQLExprTableSource) from;
            String alias = safeLower(ts.getAlias());
            String full = ts.getExpr().toString(); // 可能 db.schema.table / db.table / table
            TableName tn = TableName.parse(full);

            // 警告：若别名已存在则将被覆盖（Scope 不支持层级/撤销）
            if (scope.resolveTable(alias != null ? alias : tn.table) != null) {
                warns.add(LineageWarning.of("ALIAS_OVERRIDE",
                        "子查询别名与外层冲突，已覆盖: " + (alias != null ? alias : tn.table),
                        "FROM", "建议在子查询使用不同别名"));
            }

            scope.addTableAlias(alias != null ? alias : tn.table, tn);
            return;
        }

        if (from instanceof SQLJoinTableSource) {
            SQLJoinTableSource join = (SQLJoinTableSource) from;
            bindFrom(join.getLeft(), scope, warns);
            bindFrom(join.getRight(), scope, warns);
            if (join.getCondition() != null) {
                // ON 条件里的列也会影响子查询结果
                walk(join.getCondition(), scope, null, warns, new LinkedHashSet<>());
            }
            return;
        }
        if (from instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) from;
            SQLSelectQueryBlock query = (SQLSelectQueryBlock) sqlSubqueryTableSource.getSelect().getQuery();
            bindFrom(query.getFrom(), scope, warns);
            return;
        }

        // 子查询作为表、CTE 等暂不支持（与顶层保持一致）
        warns.add(LineageWarning.of("UNSUPPORTED_FROM",
                "FROM 子句结构暂不支持: " + from.getClass().getSimpleName(),
                from.getClass().getSimpleName(),
                "请简化 FROM 或移除复杂子查询"));
    }

    // 星号展开：qualifier=null 表示 ；否则 t.
    private void expandStar(String qualifier, Scope scope, MetadataProvider metadata, List<LineageWarning> warns, LinkedHashSet<ColumnRef> out) {
        if (qualifier == null) {
            if (scope.tableCount() == 1) {
                TableName tn = scope.getSingle();
                List<ColumnRef> cols = metadataColumns(metadata, tn, warns);
                out.addAll(cols);
            } else {
                if (warns != null) {
                    warns.add(LineageWarning.of("AMBIGUOUS_STAR",
                            "SELECT * 存在多表或无表，无法唯一展开",
                            "SELECT", "请使用 t.* 或明确列名"));
                }
            }
        } else {
            TableName tn = scope.resolveTable(qualifier);
            if (tn == null) {
                if (warns != null) {
                    warns.add(LineageWarning.of("UNKNOWN_ALIAS",
                            "无法解析别名/表名: " + qualifier,
                            "Scope", "请检查子查询 FROM/别名"));
                }
                return;
            }
            List<ColumnRef> cols = metadataColumns(metadata, tn, warns);
            out.addAll(cols);
        }
    }

    // 元数据取列清单
    private List<ColumnRef> metadataColumns(MetadataProvider metadata, TableName tn, List<LineageWarning> warns) {
        List<ColumnRef> cols = metadata.getColumns(tn.db, tn.sc, tn.table);
        if (cols == null || cols.isEmpty()) {
            if (warns != null) {
                warns.add(LineageWarning.of("METADATA_MISSING",
                        "元数据缺失: " + tn.toString(), "MetadataProvider", "请补充该表的列元数据"));
            }
            return new ArrayList<ColumnRef>();
        }
        return cols;
    }

    private String safeLower(String s) {
        return s == null ? null : s.toLowerCase(java.util.Locale.ROOT);
    }
}