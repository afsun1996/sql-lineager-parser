package com.afsun.lineage.graph;

import lombok.Data;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.RelationshipProperties;

import java.util.Objects;

@Data
public class OwnerEdge {
    @Id
    @GeneratedValue
    private Long id;

    private final ColumnNode from;

    private final TableNode to;

    public OwnerEdge(ColumnNode from, TableNode to) {
        this.from = from;
        this.to = to;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OwnerEdge)) return false;
        OwnerEdge that = (OwnerEdge) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "OwnerEdge{" + "from=" + from + ", to=" + to + '}';
    }
}