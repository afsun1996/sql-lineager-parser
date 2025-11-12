package com.afsun.lineage.core.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author afsun
 * @date 2025-11-11日 10:42
 */
public class SqlScriptUtils {
    /**
     * 移除SQL中的注释（保留字符串字面量中的内容）
     * 支持：
     * - 单行注释：-- comment 或 # comment (MySQL)
     * - 多行注释：/* comment *\/
     *
     * @param sql 原始SQL文本
     * @return 移除注释后的SQL
     */
    public static String stripComments(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        StringBuilder result = new StringBuilder(sql.length());
        int len = sql.length();
        int i = 0;

        while (i < len) {
            char c = sql.charAt(i);

            // 1. 处理字符串字面量（单引号）
            if (c == '\'') {
                result.append(c);
                i++;
                // 查找字符串结束，处理转义
                while (i < len) {
                    char ch = sql.charAt(i);
                    result.append(ch);
                    if (ch == '\'') {
                        // 检查是否是转义的单引号 ''
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                            result.append('\'');
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else if (ch == '\\' && i + 1 < len) {
                        // 反斜杠转义
                        result.append(sql.charAt(i + 1));
                        i += 2;
                    } else {
                        i++;
                    }
                }
                continue;
            }

            // 2. 处理双引号字符串（标识符）
            if (c == '"') {
                result.append(c);
                i++;
                while (i < len) {
                    char ch = sql.charAt(i);
                    result.append(ch);
                    if (ch == '"') {
                        if (i + 1 < len && sql.charAt(i + 1) == '"') {
                            result.append('"');
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else if (ch == '\\' && i + 1 < len) {
                        result.append(sql.charAt(i + 1));
                        i += 2;
                    } else {
                        i++;
                    }
                }
                continue;
            }

            // 3. 处理多行注释 /* ... */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                i += 2;
                // 查找注释结束
                while (i < len) {
                    if (sql.charAt(i) == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
                        i += 2;
                        break;
                    }
                    i++;
                }
                result.append(' '); // 用空格替代注释
                continue;
            }

            // 4. 处理单行注释 -- ...
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                // 跳过到行尾
                while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                    i++;
                }
                // 保留换行符
                if (i < len) {
                    result.append(sql.charAt(i));
                    i++;
                }
                continue;
            }

            // 5. 处理MySQL风格单行注释 # ...
            if (c == '#') {
                // 跳过到行尾
                while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                    i++;
                }
                // 保留换行符
                if (i < len) {
                    result.append(sql.charAt(i));
                    i++;
                }
                continue;
            }

            // 6. 普通字符直接追加
            result.append(c);
            i++;
        }
        return result.toString().trim();
    }


    // 语句切分（保守策略：以分号切分，保留语句顺序）
    public static List<String> splitStatements(String text) {
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray()) {
            if (c == ';') {
                list.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        if (sb.length() > 0) list.add(sb.toString().trim());
        return list;
    }

}
