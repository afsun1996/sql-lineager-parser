package com.afsun.lineage.core;

import lombok.Data;

import java.util.List;

@Data
public class ParseResult {
    private LineageGraph graph;
    private List<LineageWarning> warnings;
    private int skippedFragments;
    private String traceId;
    private long parseMillis;
}