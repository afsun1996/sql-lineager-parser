package com.afsun.lineage.core.util;

import com.afsun.lineage.core.ColumnRef;
import com.afsun.lineage.core.DefaultSqlLineageParser;
import com.afsun.lineage.core.ParseResult;
import com.afsun.lineage.core.meta.MetadataProvider;
import com.afsun.lineage.graph.TableNode;
import com.alibaba.druid.DbType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试WITH子句（CTE）的解析，特别是复杂的嵌套子查询场景
 */
class WithClauseTest {

    private DefaultSqlLineageParser parser;
    private MetadataProvider mockMetadata;

    @BeforeEach
    void setUp() {
        parser = new DefaultSqlLineageParser();

        // 创建模拟元数据提供者
        mockMetadata = (db, schema, table) -> {
            // 模拟 t_http_records_kafka 表的列
            if ("t_http_records_kafka".equalsIgnoreCase(table)) {
                return Arrays.asList(
                    ColumnRef.of(db, schema, table, "serverdata_statuscode", db, schema, table, "serverData_statusCode"),
                    ColumnRef.of(db, schema, table, "clientipaddr", db, schema, table, "clientIpAddr"),
                    ColumnRef.of(db, schema, table, "clientdata_url", db, schema, table, "clientData_URL"),
                    ColumnRef.of(db, schema, table, "clientdata_host", db, schema, table, "clientData_host"),
                    ColumnRef.of(db, schema, table, "hh", db, schema, table, "hh"),
                    ColumnRef.of(db, schema, table, "dshh", db, schema, table, "dshh"),
                    ColumnRef.of(db, schema, table, "ds", db, schema, table, "ds")
                );
            }

            // 模拟 t_ipaddress 表的列
            if ("t_ipaddress".equalsIgnoreCase(table)) {
                return Arrays.asList(
                    ColumnRef.of(db, schema, table, "ip", db, schema, table, "ip"),
                    ColumnRef.of(db, schema, table, "city", db, schema, table, "city"),
                    ColumnRef.of(db, schema, table, "isstatistic", db, schema, table, "isstatistic")
                );
            }

            // 模拟 t_system_menu_catalog 表的列
            if ("t_system_menu_catalog".equalsIgnoreCase(table)) {
                return Arrays.asList(
                    ColumnRef.of(db, schema, table, "website", db, schema, table, "website"),
                    ColumnRef.of(db, schema, table, "business_link", db, schema, table, "business_link"),
                    ColumnRef.of(db, schema, table, "status", db, schema, table, "status"),
                    ColumnRef.of(db, schema, table, "refered", db, schema, table, "refered"),
                    ColumnRef.of(db, schema, table, "feature", db, schema, table, "feature")
                );
            }

            // 模拟目标表 t_menu_url_statistic_kafka_city_tmp 的列
            if ("t_menu_url_statistic_kafka_city_tmp".equalsIgnoreCase(table)) {
                return Arrays.asList(
                    ColumnRef.of(db, schema, table, "website", db, schema, table, "website"),
                    ColumnRef.of(db, schema, table, "url", db, schema, table, "url"),
                    ColumnRef.of(db, schema, table, "count_total", db, schema, table, "count_total"),
                    ColumnRef.of(db, schema, table, "count_success", db, schema, table, "count_success"),
                    ColumnRef.of(db, schema, table, "count_fail", db, schema, table, "count_fail"),
                    ColumnRef.of(db, schema, table, "ip_count", db, schema, table, "ip_count"),
                    ColumnRef.of(db, schema, table, "city", db, schema, table, "city"),
                    ColumnRef.of(db, schema, table, "statistic_time", db, schema, table, "statistic_time")
                );
            }

            return null;
        };
    }

    @Test
    void testComplexWithClauseInsert() {
        String sql = "INSERT INTO t_menu_url_statistic_kafka_city_tmp (website, url, count_total, count_success, count_fail, ip_count,city, statistic_time)\n" +
                "        WITH\n" +
                "            raw_data AS (\n" +
                "                select\n" +
                "                    t1.*,\n" +
                "                    t2.city\n" +
                "                from\n" +
                "                    (\n" +
                "                        SELECT\n" +
                "                            serverData_statusCode,\n" +
                "                            clientIpAddr,\n" +
                "                            clientData_URL,\n" +
                "                            clientData_host,\n" +
                "                            hh,\n" +
                "                            dshh\n" +
                "                        FROM t_http_records_kafka\n" +
                "                        WHERE\n" +
                "                            ds = ''\n" +
                "                          and hh = ''\n" +
                "                          and clientIpAddr NOT IN  (select distinct ip from t_ipaddress where isstatistic='NO' AND length(ip) !=0)\n" +
                "                          and clientData_host in\n" +
                "                              (select distinct website from t_system_menu_catalog where length(website)!=0 and status = 1 and refered = '' and feature = '' )\n" +
                "                    ) t1 left join (select city,ip from t_ipaddress where city != '') t2 on t1.clientIpAddr = t2.ip\n" +
                "            )\n" +
                "        select website,url,countTotal,countSuccess,countFail, ipCount, city,curDateTime as statisticTime from\n" +
                "            (\n" +
                "                (\n" +
                "                    SELECT\n" +
                "                        COUNT(1) AS countTotal,\n" +
                "                        COUNT(CASE WHEN serverData_statusCode = '200' THEN 1 END) AS countSuccess,\n" +
                "                        COUNT(CASE WHEN serverData_statusCode != '200' THEN 1 END) AS countFail,\n" +
                "                        COUNT(DISTINCT clientIpAddr) AS ipCount,\n" +
                "                        arrayElement(splitByChar('?',clientData_URL),1) as url,\n" +
                "                        clientData_host as website,\n" +
                "                        splitByChar(':',hh)[1] as curHour,\n" +
                "                        city,\n" +
                "                        toDateTime(concat(splitByChar(' ',max(dshh))[1],' ',curHour, ':00:00')) as curDateTime\n" +
                "                    FROM raw_data\n" +
                "                    where\n" +
                "                        url in\n" +
                "                        (select distinct business_link from t_system_menu_catalog tsc\n" +
                "                         where length(tsc.business_link)!=0 and tsc.business_link not like '%?%'\n" +
                "                           and tsc.business_link not like '%^#cs#^%'  and status = 1 and refered = '' and feature = ''\n" +
                "                        )\n" +
                "                    group by url,clientData_host,curHour,city\n" +
                "                )\n" +
                "                union all\n" +
                "                (\n" +
                "                    SELECT\n" +
                "                        COUNT(1) AS countTotal,\n" +
                "                        COUNT(CASE WHEN serverData_statusCode = '200' THEN 1 END) AS countSuccess,\n" +
                "                        COUNT(CASE WHEN serverData_statusCode != '200' THEN 1 END) AS countFail,\n" +
                "                        COUNT(DISTINCT clientIpAddr) AS ipCount,\n" +
                "                        clientData_URL as url,\n" +
                "                        clientData_host as website,\n" +
                "                        splitByChar(':',hh)[1] as curHour,\n" +
                "                        city,\n" +
                "                        toDateTime(concat(splitByChar(' ',max(dshh))[1],' ',curHour, ':00:00')) as curDateTime\n" +
                "                    FROM raw_data\n" +
                "                    where\n" +
                "                        clientData_URL in\n" +
                "                        (select distinct business_link from t_system_menu_catalog tsc\n" +
                "                         where length(tsc.business_link)!=0 and tsc.business_link like '%?%'\n" +
                "                          and tsc.business_link not like '%^#cs#^%'   and status = 1 and refered = '' and feature = ''\n" +
                "                        )\n" +
                "                    group by clientData_URL,clientData_host,curHour,city\n" +
                "                )\n" +
                "            )";

        ParseResult result = parser.parse(sql, DbType.clickhouse, mockMetadata);

        assertNotNull(result);
        assertNotNull(result.getGraph());

        // 验证表节点数量：应该包含所有源表
        Set<TableNode> tables = result.getGraph().getTables();
        System.out.println("\n=== 解析到的表节点 ===");
        for (TableNode table : tables) {
            System.out.println("表: " + table.getTable() + " (原始: " + table.getOriginalTable() + ")");
        }

        // 验证是否包含所有预期的表
        assertTrue(tables.stream().anyMatch(t -> "t_menu_url_statistic_kafka_city_tmp".equalsIgnoreCase(t.getTable())),
                "应该包含目标表 t_menu_url_statistic_kafka_city_tmp");
        assertTrue(tables.stream().anyMatch(t -> "t_http_records_kafka".equalsIgnoreCase(t.getTable())),
                "应该包含源表 t_http_records_kafka");
        assertTrue(tables.stream().anyMatch(t -> "t_ipaddress".equalsIgnoreCase(t.getTable())),
                "应该包含源表 t_ipaddress");
        // 注意：t_system_menu_catalog 只在WHERE条件中用于过滤，不参与字段组成，所以不需要被解析出来

        // 验证表的数量（至少应该有3个：目标表 + 2个源表）
        assertTrue(tables.size() >= 3,
                "应该至少解析出3个表，实际解析出: " + tables.size() + " 个表");

        System.out.println("\n总共解析出 " + tables.size() + " 个表节点");
        System.out.println("列节点数量: " + result.getGraph().getColumns().size());
        System.out.println("血缘关系数量: " + result.getGraph().getToEdges().size());

        // 打印血缘关系详情
        System.out.println("\n=== 血缘关系详情 ===");
        result.getGraph().getToEdges().forEach(edge -> {
            System.out.println(edge.getFrom().getTable() + "." + edge.getFrom().getColumn() +
                             " -> " + edge.getTo().getTable() + "." + edge.getTo().getColumn());
        });
    }
}
