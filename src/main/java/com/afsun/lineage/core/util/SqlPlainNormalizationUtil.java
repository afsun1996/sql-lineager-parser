package com.afsun.lineage.core.util;

import java.util.Map;

/**
 * @author afsun
 * @date 2025-11-05日 11:26
 */
public class SqlPlainNormalizationUtil {
    /**
     * SQL处理入口
     * @param sql SQL原文
     * @return
     */
    public static String process(String sql) {
        return removeAnnotation(sql);
    }


    /**
     * 处理SQL占位符
     * @param sql SQL原文
     * @param vars 变量
     * @return
     */
    private String process(String sql, Map<String, String> vars) {
        String result = sql;
        for (Map.Entry<String, String> e : vars.entrySet()) {
            result = result.replaceAll("\\$\\{" + e.getKey() + "\\}", e.getValue());
        }
        return result;
    }


    /**
     * 删除注释内容
     * @param sql SQL原文
     * @return 输出内容
     */
    private static String removeAnnotation(String sql) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String line : sql.split("\n")) {
            if (line.indexOf("--") > 0){
                stringBuilder.append(line, 0, line.indexOf("--")).append("\n");
            }else if (line.indexOf("--") == 0) {
                continue;
            }
            else {
                stringBuilder.append(line).append("\n");
            }
        }
        return stringBuilder.toString();
    }

}
