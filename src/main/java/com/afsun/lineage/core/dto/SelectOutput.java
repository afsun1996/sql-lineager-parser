package com.afsun.lineage.core.dto;

import com.afsun.lineage.core.ColumnRef;
import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author afsun
 * @date 2025-11-11日 10:51
 */
// 选择输出的临时载体
@Data
public class SelectOutput {
    final String outputName;
    final List<ColumnRef> sources;

    public SelectOutput(String outputName, List<ColumnRef> sources) {
        this.outputName = outputName;
        // 去重：基于 ColumnRef equals/hashCode
        LinkedHashSet<ColumnRef> set = new LinkedHashSet<>(sources);
        this.sources = new ArrayList<>(set);
    }
}
