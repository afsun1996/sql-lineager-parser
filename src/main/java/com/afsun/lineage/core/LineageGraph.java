package com.afsun.lineage.core;

import com.afsun.lineage.graph.ColumnNode;
import com.afsun.lineage.graph.OwnerEdge;
import com.afsun.lineage.graph.TableNode;
import com.afsun.lineage.graph.ToEdge;
import lombok.Data;

import java.util.*;

@Data
public class LineageGraph {
    private final Set<TableNode> tables = new LinkedHashSet<>();
    private final Set<ColumnNode> columns = new LinkedHashSet<>();
    private final List<OwnerEdge> ownerEdges = new ArrayList<>();
    private final List<ToEdge> toEdges = new ArrayList<>();

    public void addOwner(ColumnNode col, TableNode tbl) {
        this.columns.add(col);
        this.tables.add(tbl);
        this.ownerEdges.add(new OwnerEdge(col, tbl));
    }

    public void addTo(ColumnNode src, ColumnNode dst) {
        this.columns.add(src);
        this.columns.add(dst);
        this.toEdges.add(new ToEdge(src, dst));
    }
}