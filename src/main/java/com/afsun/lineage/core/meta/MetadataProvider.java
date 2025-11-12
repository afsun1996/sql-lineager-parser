package com.afsun.lineage.core.meta;

import com.afsun.lineage.core.ColumnRef;

import java.util.List;

public interface MetadataProvider {
    List<ColumnRef> getColumns(String database, String schema, String table);
}