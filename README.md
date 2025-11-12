# SQL Lineage Analyzer - SQL血缘分析系统

## 项目简介

SQL Lineage Analyzer 是一个强大的SQL血缘关系分析工具，用于解析SQL脚本中表与表、列与列之间的数据血缘关系。支持多种数据库方言，提供表级和列级的血缘追踪能力。

### 核心特性

- **多方言支持**：支持 MySQL、ClickHouse、PostgreSQL、Oracle、SQL Server、ODPS 等主流数据库
- **表级血缘**：追踪表之间的依赖关系
- **列级血缘**：精确到列的数据流向分析
- **多语句解析**：支持批量SQL脚本解析
- **元数据管理**：自动加载和缓存数据库元数据
- **Neo4j集成**：将血缘关系持久化到图数据库
- **RESTful API**：提供完整的HTTP接口

## 快速开始

### 环境要求

- JDK 8+
- Maven 3.6+
- ClickHouse（用于元数据获取）
- Neo4j（可选，用于血缘关系存储）

### 配置文件

在 `application.yml` 中配置数据源：

```yaml
spring:
  datasource:
    dynamic:
      primary: clickhouse
      datasource:
        clickhouse:
          driver-class-name: ru.yandex.clickhouse.ClickHouseDriver
          url: jdbc:clickhouse://localhost:8123/default
          username: default
          password:

  neo4j:
    uri: bolt://localhost:7687
    authentication:
      username: neo4j
      password: your_password

# 元数据刷新配置
sql:
  lineage:
    metadata:
      refresh-cron: "0 0 * * * ?"  # 每小时刷新一次
    max-file-size: 10485760  # 10MB
```

### 启动应用

```bash
mvn clean install
mvn spring-boot:run
```

应用将在 `http://localhost:8080` 启动。

## API 使用指南

### 1. 解析SQL文本

**接口**：`POST /sql/analyzer/parse`

**请求体**：
```sql
CREATE TABLE test_db.user_summary AS
SELECT
    u.user_id,
    u.user_name,
    COUNT(o.order_id) as order_count
FROM test_db.users u
LEFT JOIN test_db.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.user_name;
```

**响应示例**：
```json
{
  "status": "200",
  "data": {
    "traceId": "LN-1699999999999",
    "graph": {
      "tables": [
        {
          "database": null,
          "schema": "test_db",
          "table": "users"
        },
        {
          "database": null,
          "schema": "test_db",
          "table": "orders"
        },
        {
          "database": null,
          "schema": "test_db",
          "table": "user_summary"
        }
      ],
      "columns": [...],
      "ownerEdges": [...],
      "toEdges": [...]
    },
    "warnings": [],
    "skippedFragments": 0,
    "parseMillis": 156
  },
  "message": ""
}
```

### 2. 上传SQL文件

**接口**：`POST /sql/analyzer/upload`

**请求**：
```bash
curl -X POST http://localhost:8080/sql/analyzer/upload \
  -F "file=@your_script.sql"
```

### 3. 查询表的上游依赖

**接口**：`GET /sql/lineage/table/upstream`

**参数**：
- `schema`：模式名（必填）
- `tableName`：表名（必填）
- `database`：数据库名（可选）
- `depth`：查询深度，默认1（可选）

**示例**：
```bash
curl "http://localhost:8080/sql/lineage/table/upstream?schema=test_db&tableName=user_summary&depth=2"
```

**响应示例**：
```json
{
  "status": "200",
  "data": {
    "queryType": "TABLE_UPSTREAM",
    "source": "test_db.user_summary",
    "depth": 2,
    "tableNodes": [
      {
        "database": null,
        "schema": "test_db",
        "tableName": "users",
        "level": 1
      },
      {
        "database": null,
        "schema": "test_db",
        "tableName": "orders",
        "level": 1
      }
    ],
    "edges": [...],
    "queryMillis": 45
  }
}
```

### 4. 查询列的血缘关系

**接口**：`GET /sql/lineage/column/upstream`

**参数**：
- `schema`：模式名（必填）
- `tableName`：表名（必填）
- `columnName`：列名（必填）
- `database`：数据库名（可选）
- `depth`：查询深度，默认1（可选）

**示例**：
```bash
curl "http://localhost:8080/sql/lineage/column/upstream?schema=test_db&tableName=user_summary&columnName=user_id&depth=1"
```

### 5. 刷新元数据

**接口**：`POST /sql/analyzer/metadata/reload`

**示例**：
```bash
curl -X POST http://localhost:8080/sql/analyzer/metadata/reload
```

### 6. 查看元数据统计

**接口**：`GET /sql/analyzer/metadata/stats`

**示例**：
```bash
curl http://localhost:8080/sql/analyzer/metadata/stats
```

## 支持的SQL语法

### 完全支持

- ✅ `SELECT` 查询（包括子查询、JOIN、UNION）
- ✅ `INSERT INTO ... SELECT`
- ✅ `CREATE TABLE AS SELECT` (CTAS)
- ✅ `CREATE VIEW AS SELECT`
- ✅ `UPDATE ... SET ... FROM`
- ✅ `MERGE INTO ... WHEN MATCHED`
- ✅ `WITH` (CTE - 公共表表达式)
- ✅ 星号展开 (`SELECT *`, `SELECT t.*`)

### 部分支持

- ⚠️ `CREATE TABLE` (仅提取列定义，不生成血缘)
- ⚠️ `DROP TABLE` (从临时表缓存中移除)
- ⚠️ `DELETE` (不生成列级血缘)

### 暂不支持

- ❌ 存储过程和函数
- ❌ 动态SQL
- ❌ 复杂的窗口函数血缘
- ❌ PIVOT/UNPIVOT

## 架构设计

```
sql-Lineager-enhance/
├── src/main/java/com/afsun/lineage/
│   ├── controller/          # REST API控制器
│   │   ├── AnalyzerSqlController.java
│   │   ├── LineageQueryController.java
│   │   └── GlobalExceptionHandler.java
│   ├── core/                # 核心解析引擎
│   │   ├── DefaultSqlLineageParser.java
│   │   ├── ExpressionResolver.java
│   │   ├── LineageGraph.java
│   │   ├── ParseResult.java
│   │   ├── exceptions/      # 异常定义
│   │   ├── meta/            # 元数据管理
│   │   ├── parser/          # 语句处理器
│   │   └── util/            # 工具类
│   ├── graph/               # 图节点定义
│   ├── neo4j/               # Neo4j集成
│   ├── service/             # 业务服务层
│   └── vo/                  # 值对象
└── src/test/java/           # 单元测试
```

## 错误处理

系统提供了完善的错误处理机制：

### 错误码说明

| 错误码 | HTTP状态码 | 说明 | 建议 |
|--------|-----------|------|------|
| METADATA_NOT_FOUND | 400 | 元数据未找到 | 检查表名是否正确，调用 /metadata/reload 刷新 |
| UNSUPPORTED_SYNTAX | 422 | 不支持的SQL语法 | 简化SQL语句或拆分为多个简单语句 |
| INTERNAL_PARSE_ERROR | 500 | 内部解析错误 | 联系技术支持并提供完整SQL脚本 |

### 错误响应示例

```json
{
  "status": "400",
  "data": null,
  "message": "元数据缺失: 未找到表的元数据信息: test_db.non_existent_table\n建议：1) 检查表名是否正确 2) 调用 /metadata/reload 刷新元数据"
}
```

## 性能优化建议

1. **元数据缓存**：元数据会在应用启动时自动加载，并定时刷新。对于大规模数据库，建议调整刷新频率。

2. **批量解析**：对于多个SQL文件，建议合并后一次性解析，减少元数据查询次数。

3. **查询深度控制**：血缘查询时，深度越大性能开销越大，建议从 depth=1 开始逐步增加。

4. **Neo4j索引**：如果使用Neo4j存储，建议为表名和列名创建索引：
   ```cypher
   CREATE INDEX ON :TableNode(tableName);
   CREATE INDEX ON :ColumnNode(columnName);
   ```

## 测试

运行单元测试：

```bash
mvn test
```

测试覆盖：
- ✅ 核心解析器测试
- ✅ Controller层测试
- ✅ 异常处理测试
- ✅ 元数据提供者测试

## 常见问题

### Q1: 为什么解析失败提示"元数据未找到"？

**A**: 可能原因：
1. 表不存在于ClickHouse中
2. 元数据缓存未刷新
3. 表名大小写不匹配

**解决方案**：调用 `POST /sql/analyzer/metadata/reload` 刷新元数据。

### Q2: 支持哪些数据库方言？

**A**: 系统会自动检测SQL方言，支持：
- MySQL（默认）
- ClickHouse
- PostgreSQL
- Oracle
- SQL Server
- ODPS/MaxCompute

### Q3: 如何处理临时表？

**A**: 系统支持脚本内的临时表（通过 `CREATE TABLE` 创建），会自动注册到动态元数据中。

### Q4: 血缘关系存储在哪里？

**A**:
- 内存：解析结果以 `LineageGraph` 对象返回
- Neo4j：如果配置了Neo4j，会自动持久化到图数据库

## 贡献指南

欢迎提交Issue和Pull Request！

## 许可证

本项目采用 Apache 2.0 许可证。

## 联系方式

- 作者：afsun
- 项目地址：[GitHub Repository URL]
- 问题反馈：[Issues URL]
