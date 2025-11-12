package com.afsun.lineage.core.dto;

import com.afsun.lineage.graph.TableNode;
import lombok.Data;

import java.util.List;

/**
 * @author afsun
 * @date 2025-11-11日 10:52
 */
// 目标上下文：CTAS/VIEW/INSERT...SELECT
@Data
public class TargetContext {
    final TableNode targetTable;
    final List<String> colNames;

    private TargetContext(TableNode t, List<String> cols) {
        this.targetTable = t;
        this.colNames = cols;
    }

    public static TargetContext of(TableNode t, List<String> cols) {
        return new TargetContext(t, cols);
    }
}
