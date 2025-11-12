package com.afsun.lineage.neo4j.service;

import com.afsun.lineage.core.*;
import com.afsun.lineage.core.meta.MetadataProvider;
import com.afsun.lineage.service.SqlLineageParseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author afsun
 * @date 2025-11-04日 15:17
 */
@Service
@Slf4j
public class SqlLineageParseServiceImpl implements SqlLineageParseService {
    @Resource
    private LineageService lineageService;
    @Resource
    private MetadataProvider metadataProvider;

    @Override
    public ParseResult parse(String content) {
        SqlLineageParser sqlLineageParser = new DefaultSqlLineageParser();
        ParseResult parse = sqlLineageParser.parse(content, metadataProvider);
        lineageService.saveLineageGraph(parse.getGraph());
        return parse;
    }
}
