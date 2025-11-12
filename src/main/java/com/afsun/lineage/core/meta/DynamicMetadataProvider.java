package com.afsun.lineage.core.meta;

import com.afsun.lineage.core.ColumnRef;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author afsun
 * @date 2025-11-05日 11:15
 */
// DynamicMetadataProvider.java (新增类)
@Slf4j
public class DynamicMetadataProvider implements MetadataProvider {

    // 基础元数据(来自外部)
    private final MetadataProvider baseProvider;

    // 动态注册的临时表元数据(脚本执行期间累积)
    private final Map<String, List<ColumnRef>> tempTables = new ConcurrentHashMap<>();

    public DynamicMetadataProvider(MetadataProvider baseProvider) {
        this.baseProvider = baseProvider;
    }

    @Override
    public List<ColumnRef> getColumns(String database, String schema, String table) {
        // 1. 优先查找临时表
        String key = buildKey(database, schema, table);
        if (tempTables.containsKey(key)) {
            return new ArrayList<>(tempTables.get(key));
        }

        // 2. 回退到基础元数据
        return baseProvider.getColumns(database, schema, table);
    }

    // ===== 新增:注册临时表 =====
    public void registerTempTable(String database, String schema, String table,
                                  List<ColumnRef> columns) {
        String key = buildKey(database, schema, table);
        tempTables.put(key, new ArrayList<>(columns));
        log.info("注册临时表: {} 列数={}", key, columns.size());
    }

    // ===== 新增:删除临时表 =====
    public void dropTempTable(String database, String schema, String table) {
        String key = buildKey(database, schema, table);
        tempTables.remove(key);
        log.info("删除临时表: {}", key);
    }

    private String buildKey(String db, String sc, String tb) {
        return (db == null ? "" : db.toLowerCase()) + "." +
                (sc == null ? "" : sc.toLowerCase()) + "." +
                tb.toLowerCase();
    }
}
