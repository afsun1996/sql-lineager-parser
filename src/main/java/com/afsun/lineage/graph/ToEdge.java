package com.afsun.lineage.graph;

import lombok.Data;

import java.util.Objects;

@Data
public class ToEdge {
    private final ColumnNode from;
    private final ColumnNode to;

    public ToEdge(ColumnNode from, ColumnNode to) {
        this.from = from;
        this.to = to;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ToEdge)) return false;
        ToEdge that = (ToEdge) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "ToEdge{" + "from=" + from + ", to=" + to + '}';
    }
}