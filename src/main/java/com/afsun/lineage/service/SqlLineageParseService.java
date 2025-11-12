package com.afsun.lineage.service;

import com.afsun.lineage.core.LineageGraph;
import com.afsun.lineage.core.ParseResult;
import com.alibaba.druid.DbType;

public interface SqlLineageParseService {

    ParseResult parse(String content, DbType dbType);
}
