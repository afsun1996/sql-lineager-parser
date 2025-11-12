package com.afsun.lineage.core;

import lombok.Data;

import java.util.Locale;

/**
 * @author afsun
 * @date 2025-11-03日 11:52
 */
// 规范化表名解析器
@Data
public class TableName {
    final String db, sc, table;
    final String odb, osc, otb;

    private TableName(String db, String sc, String table, String odb, String osc, String otb) {
        this.db = db;
        this.sc = sc;
        this.table = table;
        this.odb = odb;
        this.osc = osc;
        this.otb = otb;
    }

    // ===== 新增:直接构造方法 =====
    public static TableName of(String db, String sc, String table,
                               String odb, String osc, String otb) {
        return new TableName(
                db == null ? null : db.toLowerCase(Locale.ROOT),
                sc == null ? null : sc.toLowerCase(Locale.ROOT),
                table.toLowerCase(Locale.ROOT),
                odb, osc, otb
        );
    }

    public static TableName parse(String full) {
        String raw = full.trim();
        String[] parts = raw.split("\\.");
        String odb = raw, osc = null, otb = raw;
        String db = null, sc = null, tb;
        if (parts.length == 3) {
            db = parts[0].toLowerCase(Locale.ROOT);
            sc = parts[1].toLowerCase(Locale.ROOT);
            tb = parts[2].toLowerCase(Locale.ROOT);
            odb = parts[0];
            osc = parts[1];
            otb = parts[2];
        } else if (parts.length == 2) {
            db = parts[0].toLowerCase(Locale.ROOT);
            tb = parts[1].toLowerCase(Locale.ROOT);
            odb = parts[0];
            otb = parts[1];
        } else {
            tb = raw.toLowerCase(Locale.ROOT);
            otb = raw;
        }
        return new TableName(db, sc, tb, odb, osc, otb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (db != null) sb.append(db).append(".");
        if (sc != null) sb.append(sc).append(".");
        sb.append(table);
        return sb.toString();
    }
}
