package com.afsun.lineage.core;

import com.afsun.lineage.core.exceptions.InternalParseException;
import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.core.meta.MetadataProvider;

public interface SqlLineageParser {
    /**
     * 解析SQL文本，提取表和列级血缘关系
     *
     * @param sqlText SQL脚本文本
     * @param metadataProvider 元数据提供者
     * @return 解析结果，包含血缘图、警告信息等
     * @throws MetadataNotFoundException 元数据缺失异常
     * @throws UnsupportedSyntaxException 不支持的SQL语法异常
     * @throws InternalParseException 内部解析异常
     */
    ParseResult parse(String sqlText, MetadataProvider metadataProvider);
}