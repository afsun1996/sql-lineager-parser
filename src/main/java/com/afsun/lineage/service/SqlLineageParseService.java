package com.afsun.lineage.service;

import com.afsun.lineage.core.LineageGraph;
import com.afsun.lineage.core.ParseResult;

public interface SqlLineageParseService {

    ParseResult parse(String content);
}
