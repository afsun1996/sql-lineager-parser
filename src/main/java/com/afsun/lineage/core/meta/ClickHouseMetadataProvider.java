package com.afsun.lineage.core.meta;

import com.afsun.lineage.core.ColumnRef;
import com.afsun.lineage.vo.TableKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClickHouse元数据提供者
 * 从ClickHouse的system.columns表加载表和列的元数据信息
 * 支持定时刷新和手动刷新
 *
 * @author afsun
 */
@Component
@Slf4j
public class ClickHouseMetadataProvider implements MetadataProvider, ApplicationRunner {

    private final JdbcTemplate jdbcTemplate;

    /**
     * 元数据缓存：表 -> 列清单
     * 使用ConcurrentHashMap保证线程安全
     */
    private final Map<TableKey, List<ColumnRef>> columnsByTable = new ConcurrentHashMap<>();

    /**
     * 最后一次刷新时间戳
     */
    private volatile long lastReloadTime = 0;

    /**
     * 元数据总数统计
     */
    private volatile int totalTables = 0;
    private volatile int totalColumns = 0;

    @Autowired
    public ClickHouseMetadataProvider(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 应用启动时自动加载元数据
     */
    @Override
    public void run(ApplicationArguments args) {
        log.info("应用启动，开始加载ClickHouse元数据...");
        reload();
    }

    /**
     * 定时刷新元数据：每小时执行一次
     * 可通过配置文件调整刷新频率
     */
    @Scheduled(cron = "${sql.lineage.metadata.refresh-cron:0 0 * * * ?}")
    public void scheduledReload() {
        log.info("定时任务触发，开始刷新ClickHouse元数据...");
        reload();
    }

    /**
     * 手动刷新元数据
     * 加载所有非系统库的表和列信息到内存缓存
     *
     * @return 刷新结果统计信息
     */
    public synchronized Map<String, Object> reload() {
        long startTime = System.currentTimeMillis();
        Map<TableKey, List<ColumnRef>> newMap = new HashMap<>();

        try {
            // 查询ClickHouse系统表获取元数据
            final String sql =
                    "SELECT database, table, name, position " +
                    "FROM system.columns " +
                    "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA') " +
                    "ORDER BY database, table, position";

            log.debug("执行元数据查询: {}", sql);

            jdbcTemplate.query(sql, rs -> {
                String ckDb = rs.getString("database");
                String ckTable = rs.getString("table");
                String ckColumn = rs.getString("name");

                // 约定：ColumnRef.database=null，ColumnRef.schema=ckDb
                TableKey key = new TableKey(null, ckDb, ckTable);
                List<ColumnRef> list = newMap.computeIfAbsent(key, k -> new ArrayList<>());

                // original* 保留原值；of() 内部会对 db/sc/tb/col 做 toLowerCase
                list.add(ColumnRef.of(
                        null, ckDb, ckTable, ckColumn,  // 逻辑层面的 db/schema/table/column
                        null, ckDb, ckTable, ckColumn   // original*
                ));
            });

            // 原子替换旧缓存
            columnsByTable.clear();
            newMap.forEach((k, v) -> columnsByTable.put(k, Collections.unmodifiableList(v)));

            // 更新统计信息
            totalTables = newMap.size();
            totalColumns = newMap.values().stream().mapToInt(List::size).sum();
            lastReloadTime = System.currentTimeMillis();

            long elapsed = lastReloadTime - startTime;
            log.info("元数据刷新完成！加载 {} 张表，{} 个列，耗时 {}ms",
                    totalTables, totalColumns, elapsed);

            // 返回统计信息
            Map<String, Object> stats = new HashMap<>();
            stats.put("success", true);
            stats.put("tables", totalTables);
            stats.put("columns", totalColumns);
            stats.put("elapsedMs", elapsed);
            stats.put("timestamp", new Date(lastReloadTime));
            return stats;

        } catch (Exception e) {
            log.error("元数据刷新失败", e);
            Map<String, Object> stats = new HashMap<>();
            stats.put("success", false);
            stats.put("error", e.getMessage());
            return stats;
        }
    }

    /**
     * 获取指定表的列清单
     *
     * @param database 数据库名（可为null）
     * @param schema   模式名（ClickHouse中即database）
     * @param table    表名
     * @return 列引用清单，若表不存在则返回空列表
     */
    @Override
    public List<ColumnRef> getColumns(String database, String schema, String table) {
        // 对入参归一化：优先使用 schema 作为 CK database；schema 为空则用 database
        String ckDb = schema != null ? schema : database;
        if (table == null || ckDb == null) {
            return Collections.emptyList();
        }
        TableKey key = new TableKey(null, ckDb, table);
        return columnsByTable.getOrDefault(key, Collections.emptyList());
    }

    /**
     * 获取元数据统计信息
     *
     * @return 包含表数、列数、最后刷新时间等信息的Map
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalTables", totalTables);
        stats.put("totalColumns", totalColumns);
        stats.put("lastReloadTime", lastReloadTime > 0 ? new Date(lastReloadTime) : null);
        stats.put("cacheSize", columnsByTable.size());
        return stats;
    }

    /**
     * 检查指定表是否存在于元数据缓存中
     *
     * @param database 数据库名
     * @param schema   模式名
     * @param table    表名
     * @return 是否存在
     */
    public boolean hasTable(String database, String schema, String table) {
        String ckDb = schema != null ? schema : database;
        if (table == null || ckDb == null) {
            return false;
        }
        TableKey key = new TableKey(null, ckDb, table);
        return columnsByTable.containsKey(key);
    }
}