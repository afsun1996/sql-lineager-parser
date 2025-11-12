package com.afsun.lineage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * SQL血缘分析应用主类
 *
 * @author afsun
 * @date 2025-11-03日 16:02
 */
@SpringBootApplication(scanBasePackages = "com.afsun.lineage")
@ConfigurationPropertiesScan
@EnableScheduling
public class SqlLineageAnalyzerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SqlLineageAnalyzerApplication.class, args);
    }
}
