package com.afsun.lineage.core.util;

import com.alibaba.druid.DbType;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

/**
 * SQL方言检测器
 * 根据SQL文本特征智能识别数据库方言类型
 *
 * @author afsun
 */
@Slf4j
public class SqlDialectDetector {

    /**
     * 检测SQL方言类型
     * 支持：MySQL、ClickHouse、ODPS、PostgreSQL、Oracle、SQLServer等
     *
     * @param sql SQL文本
     * @return 检测到的方言类型
     */
    public static DbType detect(String sql) {
        if (sql == null || sql.isEmpty()) {
            return DbType.mysql;
        }

        String s = sql.toLowerCase(Locale.ROOT);

        // 1. ODPS/MaxCompute特征检测
        if (containsOdpsFeatures(s)) {
            log.debug("检测到ODPS方言特征");
            return DbType.odps;
        }

        // 2. ClickHouse特征检测
        if (containsClickHouseFeatures(s)) {
            log.debug("检测到ClickHouse方言特征");
            return DbType.clickhouse;
        }

        // 3. PostgreSQL特征检测
        if (containsPostgreSQLFeatures(s)) {
            log.debug("检测到PostgreSQL方言特征");
            return DbType.postgresql;
        }

        // 4. Oracle特征检测
        if (containsOracleFeatures(s)) {
            log.debug("检测到Oracle方言特征");
            return DbType.oracle;
        }

        // 5. SQLServer特征检测
        if (containsSQLServerFeatures(s)) {
            log.debug("检测到SQLServer方言特征");
            return DbType.sqlserver;
        }

        // 6. 默认使用MySQL（最通用）
        log.debug("使用默认MySQL方言");
        return DbType.mysql;
    }

    /**
     * 检测ODPS特征
     */
    private static boolean containsOdpsFeatures(String sql) {
        return sql.contains("merge into") ||
               sql.contains("distribute by") ||
               sql.contains("cluster by") ||
               sql.contains("pt partition") ||
               sql.contains("insert overwrite table");
    }

    /**
     * 检测ClickHouse特征
     */
    private static boolean containsClickHouseFeatures(String sql) {
        return sql.contains("engine =") ||
               sql.contains("engine=") ||
               (sql.contains("materialize") && sql.contains("clickhouse")) ||
               sql.contains("sample by") ||
               sql.contains("prewhere") ||
               sql.contains("final");
    }

    /**
     * 检测PostgreSQL特征
     */
    private static boolean containsPostgreSQLFeatures(String sql) {
        return (sql.contains("returning") && sql.contains("insert")) ||
               sql.contains("::") ||  // 类型转换语法
               sql.contains("lateral") ||
               sql.contains("generate_series");
    }

    /**
     * 检测Oracle特征
     */
    private static boolean containsOracleFeatures(String sql) {
        return sql.contains("dual") ||
               sql.contains("sysdate") ||
               sql.contains("rownum") ||
               sql.contains("connect by") ||
               sql.contains("start with");
    }

    /**
     * 检测SQLServer特征
     */
    private static boolean containsSQLServerFeatures(String sql) {
        return (sql.contains("top ") && sql.contains("select")) ||
               sql.contains("[dbo]") ||
               sql.contains("with (nolock)");
    }
}
