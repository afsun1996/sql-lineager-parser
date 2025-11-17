package com.afsun.lineage.core.util;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClickHouse SQL重写器
 * 将ClickHouse特有的语法转换为Druid能够解析的标准SQL语法
 *
 * @author afsun
 * @date 2025-11-17
 */
@Slf4j
public class ClickHouseSqlRewriter {

    /**
     * 匹配 INSERT INTO ... WITH ... SELECT 模式
     * 捕获组：
     * 1. INSERT INTO table_name (columns)
     * 2. WITH ... AS (...)
     * 3. SELECT ...
     */
    private static final Pattern INSERT_WITH_PATTERN = Pattern.compile(
        "(?i)(INSERT\\s+INTO\\s+[^\\s(]+\\s*(?:\\([^)]+\\))?\\s*)\\s*" +  // INSERT INTO table(cols)
        "(WITH\\s+.+?)\\s*" +                                              // WITH clause
        "(SELECT\\s+.+)",                                                  // SELECT clause
        Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    /**
     * 重写ClickHouse SQL为标准SQL
     *
     * @param sql 原始SQL
     * @return 重写后的SQL
     */
    public static String rewrite(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        // 检测是否是 INSERT INTO ... WITH ... SELECT 模式
        if (isInsertWithSelect(sql)) {
            String rewritten = rewriteInsertWithSelect(sql);
            if (rewritten != null && !rewritten.equals(sql)) {
                log.info("ClickHouse SQL已重写: INSERT WITH -> WITH + INSERT SELECT");
                log.debug("原始SQL: {}", sql.substring(0, Math.min(200, sql.length())));
                log.debug("重写SQL: {}", rewritten.substring(0, Math.min(200, rewritten.length())));
                return rewritten;
            }
        }

        return sql;
    }

    /**
     * 检测是否是 INSERT INTO ... WITH ... SELECT 模式
     */
    private static boolean isInsertWithSelect(String sql) {
        String normalized = sql.trim().replaceAll("\\s+", " ");
        String lower = normalized.toLowerCase();

        // 必须以 INSERT INTO 开头
        if (!lower.startsWith("insert into")) {
            return false;
        }

        // 查找 WITH 关键字的位置
        int withPos = findKeywordPosition(normalized, "WITH");
        if (withPos == -1) {
            return false;
        }

        // 查找 SELECT 关键字的位置
        int selectPos = findKeywordPosition(normalized, "SELECT");
        if (selectPos == -1) {
            return false;
        }

        // WITH 必须在 SELECT 之前
        return withPos < selectPos;
    }

    /**
     * 重写 INSERT INTO ... WITH ... SELECT 为 WITH ... INSERT INTO ... SELECT
     */
    private static String rewriteInsertWithSelect(String sql) {
        try {
            String normalized = sql.trim();

            // 1. 找到WITH关键字位置
            int withPos = findKeywordPosition(normalized, "WITH");
            if (withPos == -1) {
                return sql;
            }

            // 2. 提取INSERT部分
            String insertPart = normalized.substring(0, withPos).trim();

            // 3. 从WITH开始，找到完整的WITH子句（需要匹配括号）
            String afterWith = normalized.substring(withPos);
            int withEndPos = findWithClauseEnd(afterWith);

            if (withEndPos == -1) {
                return sql;
            }

            // 4. 提取WITH子句和SELECT部分
            String withPart = afterWith.substring(0, withEndPos).trim();
            String selectPart = afterWith.substring(withEndPos).trim();

            // 5. 重组为: WITH ... INSERT INTO ... SELECT ...
            String rewritten = withPart + " " + insertPart + " " + selectPart;

            return rewritten;

        } catch (Exception e) {
            log.warn("重写INSERT WITH SELECT失败，返回原始SQL: {}", e.getMessage());
            return sql;
        }
    }

    /**
     * 找到WITH子句的结束位置（需要正确匹配括号）
     * 返回相对于传入字符串的位置
     */
    private static int findWithClauseEnd(String sqlFromWith) {
        int pos = 0;
        int parenDepth = 0;
        boolean inString = false;
        char stringChar = 0;

        // 跳过 "WITH" 关键字
        pos = findKeywordPosition(sqlFromWith, "WITH");
        if (pos == -1) {
            return -1;
        }
        pos += 4; // "WITH".length()

        // 跳过空白
        while (pos < sqlFromWith.length() && Character.isWhitespace(sqlFromWith.charAt(pos))) {
            pos++;
        }

        // 遍历字符，匹配括号，找到WITH子句结束位置
        while (pos < sqlFromWith.length()) {
            char c = sqlFromWith.charAt(pos);

            // 处理字符串字面量
            if ((c == '\'' || c == '"') && (pos == 0 || sqlFromWith.charAt(pos - 1) != '\\')) {
                if (!inString) {
                    inString = true;
                    stringChar = c;
                } else if (c == stringChar) {
                    inString = false;
                }
                pos++;
                continue;
            }

            if (!inString) {
                // 处理括号
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                }

                // 当括号平衡且遇到SELECT关键字时，WITH子句结束
                if (parenDepth == 0) {
                    // 检查是否是SELECT关键字
                    if (pos + 6 <= sqlFromWith.length()) {
                        String substr = sqlFromWith.substring(pos, pos + 6).toLowerCase();
                        if (substr.equals("select")) {
                            // 确保SELECT前后是空白或边界
                            boolean validBefore = (pos == 0 || Character.isWhitespace(sqlFromWith.charAt(pos - 1)));
                            boolean validAfter = (pos + 6 >= sqlFromWith.length() ||
                                                Character.isWhitespace(sqlFromWith.charAt(pos + 6)));
                            if (validBefore && validAfter) {
                                return pos;
                            }
                        }
                    }
                }
            }

            pos++;
        }

        return -1;
    }

    /**
     * 查找关键字位置（忽略字符串字面量和注释中的关键字）
     */
    private static int findKeywordPosition(String sql, String keyword) {
        String lower = sql.toLowerCase();
        String keywordLower = keyword.toLowerCase();

        int pos = 0;
        boolean inString = false;
        char stringChar = 0;

        while (pos < sql.length()) {
            char c = sql.charAt(pos);

            // 处理字符串字面量
            if ((c == '\'' || c == '"') && (pos == 0 || sql.charAt(pos - 1) != '\\')) {
                if (!inString) {
                    inString = true;
                    stringChar = c;
                } else if (c == stringChar) {
                    inString = false;
                }
                pos++;
                continue;
            }

            // 在字符串外查找关键字
            if (!inString) {
                // 检查是否匹配关键字
                if (pos + keywordLower.length() <= sql.length()) {
                    String substr = lower.substring(pos, pos + keywordLower.length());
                    if (substr.equals(keywordLower)) {
                        // 确保关键字前后是空白或边界
                        boolean validBefore = (pos == 0 || Character.isWhitespace(sql.charAt(pos - 1)));
                        boolean validAfter = (pos + keywordLower.length() >= sql.length() ||
                                            Character.isWhitespace(sql.charAt(pos + keywordLower.length())));
                        if (validBefore && validAfter) {
                            return pos;
                        }
                    }
                }
            }

            pos++;
        }

        return -1;
    }

    /**
     * 测试方法
     */
    public static void main(String[] args) {
        String testSql = "INSERT INTO t_menu_url_statistic_kafka_city_tmp (website, url, count_total) " +
                        "WITH raw_data AS (SELECT * FROM t_http_records_kafka WHERE ds = '2024') " +
                        "SELECT website, url, COUNT(*) FROM raw_data GROUP BY website, url";

        System.out.println("原始SQL:");
        System.out.println(testSql);
        System.out.println("\n重写后SQL:");
        System.out.println(rewrite(testSql));
    }
}
