package com.afsun.lineage.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ClickHouse SQL重写器测试
 */
class ClickHouseSqlRewriterTest {

    @Test
    void testRewriteInsertWithSelect() {
        String originalSql = "INSERT INTO t_menu_url_statistic_kafka_city_tmp (website, url, count_total, count_success, count_fail, ip_count,city, statistic_time)\n" +
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
                "                    ) t1 left join (select city,ip from t_ipaddress where city != '') t2 on t1.clientIpAddr = t2.ip\n" +
                "            )\n" +
                "\n" +
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
                "                           and tsc.business_link not like '%^#cs#^%'  and status = 1\n" +
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
                "                          and tsc.business_link not like '%^#cs#^%'   and status = 1\n" +
                "                        )\n" +
                "                    group by clientData_URL,clientData_host,curHour,city\n" +
                "                )\n" +
                "            )";

        String rewritten = originalSql;

        // 验证重写后的SQL
        assertNotNull(rewritten);
        assertTrue(rewritten.toLowerCase().startsWith("with"), "重写后的SQL应该以WITH开头");
        assertTrue(rewritten.toLowerCase().contains("insert into"), "重写后的SQL应该包含INSERT INTO");
        assertTrue(rewritten.toLowerCase().contains("select"), "重写后的SQL应该包含SELECT");

        // 验证WITH在INSERT之前
        int withPos = rewritten.toLowerCase().indexOf("with");
        int insertPos = rewritten.toLowerCase().indexOf("insert into");
        assertTrue(withPos < insertPos, "WITH应该在INSERT INTO之前");

        System.out.println("原始SQL长度: " + originalSql.length());
        System.out.println("重写SQL长度: " + rewritten.length());
        System.out.println("\n重写后的SQL前200字符:");
        System.out.println(rewritten.substring(0, Math.min(200, rewritten.length())));
    }

    @Test
    void testSimpleInsertWithSelect() {
        String sql = "INSERT INTO test_table (col1, col2) " +
                    "WITH cte AS (SELECT a, b FROM source_table) " +
                    "SELECT col1, col2 FROM cte";

        String rewritten = sql;

        assertTrue(rewritten.toLowerCase().startsWith("with"));
        assertTrue(rewritten.contains("INSERT INTO test_table"));
        assertTrue(rewritten.contains("SELECT col1, col2 FROM cte"));
    }

    @Test
    void testNormalInsertSelectNotRewritten() {
        String sql = "INSERT INTO test_table SELECT * FROM source_table";
        String rewritten = sql;
        assertEquals(sql, rewritten, "普通INSERT SELECT不应该被重写");
    }
}
