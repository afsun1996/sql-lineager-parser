package com.afsun.lineage.core;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 语句级作用域：维护 FROM 中的别名到表的映射，支持限定/未限定列解析。
 */
public class Scope {

    private final Map<String, TableName> alias2Table = new LinkedHashMap<>();
    private final Map<String, String> cteMap = new LinkedHashMap<>();
    private final Map<String, String> outputAliasToExpr = new LinkedHashMap<>();
    // ===== 新增:子查询列映射 =====
    // key: "子查询别名.列名" (如 "basedata.counttotal")
    // value: 该列的源列引用
    private final Map<String, List<ColumnRef>> subqueryColumns = new LinkedHashMap<>();
    public void addSubqueryColumn(String subAlias, String colName, List<ColumnRef> sources) {
        String key = buildSubqueryColKey(subAlias, colName);
        subqueryColumns.put(key, new ArrayList<>(sources));
    }
    public List<ColumnRef> getSubqueryColumnSources(String subAlias, String colName) {
        String key = buildSubqueryColKey(subAlias, colName);
        return subqueryColumns.get(key);
    }
    private String buildSubqueryColKey(String alias, String col) {
        return (alias == null ? "" : alias.toLowerCase(Locale.ROOT))
                + "." + col.toLowerCase(Locale.ROOT);
    }
    // 添加别名映射（给定解析好的表名）
    public void addTableAlias(String aliasOrName, TableName tn) {
        String key = aliasOrName == null ? tn.table : aliasOrName.toLowerCase(Locale.ROOT);
        alias2Table.put(key, tn);
    }

    // 添加别名映射（给定完整字符串，如 db.schema.table 或 table）
    public void addTableAlias(String aliasOrName, String fullName) {
        TableName tn = TableName.parse(fullName);
        addTableAlias(aliasOrName, tn);
    }

    public int tableCount() {
        return alias2Table.size();
    }

    public Set<String> getAllTableAliases() {
        return alias2Table.keySet();
    }

    public TableName getSingle() {
        return alias2Table.values().stream().findFirst().orElse(null);
    }

    public TableName resolveTable(String aliasOrName) {
        if (aliasOrName == null) return null;
        TableName tn = alias2Table.get(aliasOrName.toLowerCase(Locale.ROOT));
        if (tn != null) return tn;
        // 尝试直接匹配真实表名
        for (TableName t : alias2Table.values()) {
            if (t.table.equalsIgnoreCase(aliasOrName)) return t;
        }
        return null;
    }

//    // 解析限定列 t.col
//    public ColumnRef resolveQualifiedColumn(String qualifier, String column, List warns) {
//        TableName tn = resolveTable(qualifier);
//        if (tn == null) {
//            if (warns != null) {
//                warns.add(LineageWarning.of("UNKNOWN_ALIAS",
//                        "未知别名/表名: " + qualifier, "Scope", "请检查 FROM/别名"));
//            }
//            return null;
//        }
//        return ColumnRef.of(tn.db, tn.sc, tn.table, column.toLowerCase(Locale.ROOT),
//                tn.odb, tn.osc, tn.otb, column);
//    }

    // ===== 修改:resolveQualifiedColumn支持子查询列 =====
    public ColumnRef resolveQualifiedColumn(String qualifier, String column, List<LineageWarning> warns) {
        // 1. 先尝试解析为子查询列
        List<ColumnRef> subSources = getSubqueryColumnSources(qualifier, column);
        if (subSources != null && !subSources.isEmpty()) {
            // 返回第一个源列(或合并多个源)
            // 这里简化处理:返回第一个
            return subSources.get(0);
        }
        // 2. 回退到普通表列
        TableName tn = resolveTable(qualifier);
        if (tn == null) {
            if (warns != null) {
                warns.add(LineageWarning.of("UNKNOWN_ALIAS",
                        "未知别名/表名: " + qualifier, "Scope", "请检查 FROM/别名"));
            }
            return null;
        }
        return ColumnRef.of(tn.db, tn.sc, tn.table, column.toLowerCase(Locale.ROOT),
                tn.odb, tn.osc, tn.otb, column);
    }

    // 解析未限定列 col：仅在单表作用域下允许
    public ColumnRef resolveUnqualifiedColumn(String column, List warns) {
        if (tableCount() == 1) {
            TableName tn = getSingle();
            return ColumnRef.of(tn.db, tn.sc, tn.table, column.toLowerCase(Locale.ROOT),
                    tn.odb, tn.osc, tn.otb, column);
        }
        if (warns != null) {
            warns.add(LineageWarning.of("AMBIGUOUS_COL",
                    "未限定列名在多表作用域中不唯一: " + column, "Scope", "请使用 t.col 形式"));
        }
        return null;
    }

    // 统一入口：解析列（带或不带限定符）
    public ColumnRef resolveColumn(String tableAliasOrName, String column) {
        if (tableAliasOrName == null || tableAliasOrName.isEmpty()) {
            return resolveUnqualifiedColumn(column, null);
        }
        return resolveQualifiedColumn(tableAliasOrName, column, null);
    }

    // 记录 CTE（WITH 名称到子查询 SQL 文本）
    public void putCTE(String name, String selectSql) {
        if (name != null && selectSql != null) {
            cteMap.put(name.toLowerCase(Locale.ROOT), selectSql);
        }
    }

    public Map<String, String> getCteMap() {
        return Collections.unmodifiableMap(cteMap);
    }

    public void putOutputAliasExpr(String alias, String exprText) {
        if (alias != null) outputAliasToExpr.put(alias.toLowerCase(Locale.ROOT), exprText);
    }

    public Map<String, String> getOutputAliasToExpr() {
        return Collections.unmodifiableMap(outputAliasToExpr);
    }

    // UNION 对齐（简化：按位置对齐，长度不一则按最短对齐）
    public List alignUnionOutputs(List branch1Cols, List branch2Cols,
                                  List warns) {
        if (branch1Cols == null) branch1Cols = Collections.emptyList();
        if (branch2Cols == null) branch2Cols = Collections.emptyList();
        int n = Math.min(branch1Cols.size(), branch2Cols.size());
        if (branch1Cols.size() != branch2Cols.size() && warns != null) {
            warns.add(LineageWarning.of("UNION_MISMATCH",
                    "UNION 分支列数不一致，按最短长度位置对齐", "UNION", "建议显式别名并保持列数一致"));
        }
        return branch1Cols.subList(0, n);
    }
}