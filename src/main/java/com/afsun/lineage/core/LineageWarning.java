package com.afsun.lineage.core;

import lombok.Data;

@Data
public class LineageWarning {
    private final String category;
    private final String summary;
    private final String position;
    private final String suggestion;

    private LineageWarning(String c, String s, String p, String sug) {
        this.category = c;
        this.summary = s;
        this.position = p;
        this.suggestion = sug;
    }

    public static LineageWarning of(String c, String s, String p, String sug) {
        return new LineageWarning(c, s, p, sug);
    }

}