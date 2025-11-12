# SQL Lineage Analyzer API 文档

## 目录

- [1. SQL解析接口](#1-sql解析接口)
- [2. 血缘查询接口](#2-血缘查询接口)
- [3. 元数据管理接口](#3-元数据管理接口)
- [4. 数据模型](#4-数据模型)
- [5. 错误码说明](#5-错误码说明)

---

## 1. SQL解析接口

### 1.1 解析SQL文本

解析SQL脚本文本，提取表级和列级血缘关系。

**接口地址**：`POST /sql/analyzer/parse`

**Content-Type**：`text/plain`

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| sqlText | String | 是 | SQL脚本文本（请求体） |

**请求示例**：

```bash
curl -X POST http://localhost:8080/sql/analyzer/parse \
  -H "Content-Type: text/plain" \
  -d "INSERT INTO test_db.target_table(id, name, age)
      SELECT user_id, user_name, user_age
      FROM test_db.source_table"
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
          "table": "source_table",
          "originalDatabase": null,
          "originalSchema": "test_db",
          "originalTable": "source_table"
        },
        {
          "database": null,
          "schema": "test_db",
          "table": "target_table",
          "originalDatabase": null,
          "originalSchema": "test_db",
          "originalTable": "target_table"
        }
      ],
      "columns": [
        {
          "database": null,
          "schema": "test_db",
          "table": "source_table",
          "column": "user_id",
          "originalDatabase": null,
          "originalSchema": "test_db",
          "originalTable": "source_table",
          "originalColumn": "user_id"
        },
        {
          "database": null,
          "schema": "test_db",
          "table": "target_table",
          "column": "id",
          "originalDatabase": null,
          "originalSchema": "test_db",
          "originalTable": "target_table",
          "originalColumn": "id"
        }
      ],
      "ownerEdges": [
        {
          "column": {
            "schema": "test_db",
            "table": "source_table",
            "column": "user_id"
          },
          "table": {
            "schema": "test_db",
            "table": "source_table"
          }
        }
      ],
      "toEdges": [
        {
          "from": {
            "schema": "test_db",
            "table": "target_table",
            "column": "id"
          },
          "to": {
            "schema": "test_db",
            "table": "source_table",
            "column": "user_id"
          }
        }
      ]
    },
    "warnings": [],
    "skippedFragments": 0,
    "parseMillis": 156
  },
  "message": ""
}
```

**复杂SQL示例**：

```sql
-- 示例1: CTAS (Create Table As Select)
CREATE TABLE test_db.user_summary AS
SELECT
    u.user_id,
    u.user_name,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_amount
FROM test_db.users u
LEFT JOIN test_db.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.user_name;

-- 示例2: WITH子句 (CTE)
WITH active_users AS (
    SELECT user_id, user_name
    FROM test_db.users
    WHERE status = 'active'
),
recent_orders AS (
    SELECT user_id, order_id, amount
    FROM test_db.orders
    WHERE order_date >= '2024-01-01'
)
INSERT INTO test_db.user_order_summary
SELECT
    au.user_id,
    au.user_name,
    COUNT(ro.order_id) as order_count
FROM active_users au
LEFT JOIN recent_orders ro ON au.user_id = ro.user_id
GROUP BY au.user_id, au.user_name;

-- 示例3: UNION
INSERT INTO test_db.all_transactions
SELECT transaction_id, amount, 'online' as channel
FROM test_db.online_transactions
UNION ALL
SELECT transaction_id, amount, 'offline' as channel
FROM test_db.offline_transactions;
```

---

### 1.2 上传SQL文件解析

上传SQL脚本文件进行解析。

**接口地址**：`POST /sql/analyzer/upload`

**Content-Type**：`multipart/form-data`

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| file | File | 是 | SQL脚本文件（UTF-8编码，最大10MB） |

**请求示例**：

```bash
curl -X POST http://localhost:8080/sql/analyzer/upload \
  -F "file=@/path/to/your/script.sql"
```

**响应格式**：与 1.1 相同

**错误示例**：

```json
{
  "status": "500",
  "data": null,
  "message": "文件大小超过限制：15.50MB > 10.00MB"
}
```

---

## 2. 血缘查询接口

### 2.1 查询表的上游依赖

查询指定表的数据来源（上游表）。

**接口地址**：`GET /sql/lineage/table/upstream`

**请求参数**：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| schema | String | 是 | - | 模式名/数据库名 |
| tableName | String | 是 | - | 表名 |
| database | String | 否 | null | 数据库名（多级命名空间） |
| depth | Integer | 否 | 1 | 查询深度（1-10） |

**请求示例**：

```bash
# 查询直接上游（depth=1）
curl "http://localhost:8080/sql/lineage/table/upstream?schema=test_db&tableName=user_summary&depth=1"

# 查询多级上游（depth=3）
curl "http://localhost:8080/sql/lineage/table/upstream?schema=test_db&tableName=final_report&depth=3"
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
      },
      {
        "database": null,
        "schema": "test_db",
        "tableName": "user_profiles",
        "level": 2
      }
    ],
    "columnNodes": [],
    "edges": [
      {
        "sourceNode": "test_db.users",
        "targetNode": "test_db.user_summary",
        "edgeType": "TABLE_DEPENDENCY"
      },
      {
        "sourceNode": "test_db.orders",
        "targetNode": "test_db.user_summary",
        "edgeType": "TABLE_DEPENDENCY"
      }
    ],
    "queryMillis": 45
  },
  "message": ""
}
```

---

### 2.2 查询表的下游依赖

查询指定表的数据流向（下游表）。

**接口地址**：`GET /sql/lineage/table/downstream`

**请求参数**：与 2.1 相同

**请求示例**：

```bash
curl "http://localhost:8080/sql/lineage/table/downstream?schema=test_db&tableName=users&depth=2"
```

**响应格式**：与 2.1 类似，`queryType` 为 `TABLE_DOWNSTREAM`

---

### 2.3 查询列的上游依赖

查询指定列的数据来源（上游列）。

**接口地址**：`GET /sql/lineage/column/upstream`

**请求参数**：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| schema | String | 是 | - | 模式名 |
| tableName | String | 是 | - | 表名 |
| columnName | String | 是 | - | 列名 |
| database | String | 否 | null | 数据库名 |
| depth | Integer | 否 | 1 | 查询深度（1-10） |

**请求示例**：

```bash
curl "http://localhost:8080/sql/lineage/column/upstream?schema=test_db&tableName=user_summary&columnName=order_count&depth=1"
```

**响应示例**：

```json
{
  "status": "200",
  "data": {
    "queryType": "COLUMN_UPSTREAM",
    "source": "test_db.user_summary.order_count",
    "depth": 1,
    "tableNodes": [],
    "columnNodes": [
      {
        "database": null,
        "schema": "test_db",
        "tableName": "orders",
        "columnName": "order_id",
        "level": 1
      }
    ],
    "edges": [
      {
        "sourceNode": "test_db.orders.order_id",
        "targetNode": "test_db.user_summary.order_count",
        "edgeType": "TO"
      }
    ],
    "queryMillis": 32
  },
  "message": ""
}
```

---

### 2.4 查询列的下游依赖

查询指定列的数据流向（下游列）。

**接口地址**：`GET /sql/lineage/column/downstream`

**请求参数**：与 2.3 相同

**请求示例**：

```bash
curl "http://localhost:8080/sql/lineage/column/downstream?schema=test_db&tableName=users&columnName=user_id&depth=2"
```

**响应格式**：与 2.3 类似，`queryType` 为 `COLUMN_DOWNSTREAM`

---

### 2.5 查询血缘路径

查询两个表之间的血缘传递路径。

**接口地址**：`GET /sql/lineage/path`

**请求参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| sourceSchema | String | 是 | 源表模式名 |
| sourceTable | String | 是 | 源表名 |
| targetSchema | String | 是 | 目标表模式名 |
| targetTable | String | 是 | 目标表名 |
| sourceDatabase | String | 否 | 源表数据库名 |
| targetDatabase | String | 否 | 目标表数据库名 |

**请求示例**：

```bash
curl "http://localhost:8080/sql/lineage/path?sourceSchema=test_db&sourceTable=users&targetSchema=test_db&targetTable=final_report"
```

**响应示例**：

```json
{
  "status": "200",
  "data": [
    [
      "test_db.users",
      "test_db.user_summary",
      "test_db.final_report"
    ],
    [
      "test_db.users",
      "test_db.user_orders",
      "test_db.order_summary",
      "test_db.final_report"
    ]
  ],
  "message": ""
}
```

---

## 3. 元数据管理接口

### 3.1 刷新元数据缓存

手动触发元数据刷新，从ClickHouse重新加载表和列信息。

**接口地址**：`POST /sql/analyzer/metadata/reload`

**请求示例**：

```bash
curl -X POST http://localhost:8080/sql/analyzer/metadata/reload
```

**响应示例**：

```json
{
  "status": "200",
  "data": {
    "success": true,
    "tables": 150,
    "columns": 1200,
    "elapsedMs": 2345,
    "timestamp": "2024-11-11T10:30:00.000+00:00"
  },
  "message": ""
}
```

**失败响应**：

```json
{
  "status": "500",
  "data": null,
  "message": "元数据刷新失败: Connection refused"
}
```

---

### 3.2 查看元数据统计

获取当前元数据缓存的统计信息。

**接口地址**：`GET /sql/analyzer/metadata/stats`

**请求示例**：

```bash
curl http://localhost:8080/sql/analyzer/metadata/stats
```

**响应示例**：

```json
{
  "status": "200",
  "data": {
    "totalTables": 150,
    "totalColumns": 1200,
    "lastReloadTime": "2024-11-11T10:30:00.000+00:00",
    "cacheSize": 150
  },
  "message": ""
}
```

---

## 4. 数据模型

### 4.1 ParseResult（解析结果）

```json
{
  "traceId": "String - 追踪ID",
  "graph": "LineageGraph - 血缘图对象",
  "warnings": "List<LineageWarning> - 警告列表",
  "skippedFragments": "Integer - 跳过的语句数",
  "parseMillis": "Long - 解析耗时（毫秒）"
}
```

### 4.2 LineageGraph（血缘图）

```json
{
  "tables": "Set<TableNode> - 表节点集合",
  "columns": "Set<ColumnNode> - 列节点集合",
  "ownerEdges": "List<OwnerEdge> - 列属于表的关系",
  "toEdges": "List<ToEdge> - 列到列的血缘关系"
}
```

### 4.3 TableNode（表节点）

```json
{
  "database": "String - 数据库名（可为null）",
  "schema": "String - 模式名",
  "table": "String - 表名（小写）",
  "originalDatabase": "String - 原始数据库名",
  "originalSchema": "String - 原始模式名",
  "originalTable": "String - 原始表名（保留大小写）"
}
```

### 4.4 ColumnNode（列节点）

```json
{
  "database": "String - 数据库名",
  "schema": "String - 模式名",
  "table": "String - 表名",
  "column": "String - 列名（小写）",
  "originalDatabase": "String - 原始数据库名",
  "originalSchema": "String - 原始模式名",
  "originalTable": "String - 原始表名",
  "originalColumn": "String - 原始列名（保留大小写）"
}
```

### 4.5 LineageWarning（警告信息）

```json
{
  "warningType": "String - 警告类型",
  "message": "String - 警告消息",
  "position": "String - 位置信息",
  "suggestion": "String - 建议"
}
```

---

## 5. 错误码说明

### 5.1 业务错误码

| 错误码 | HTTP状态码 | 说明 | 解决方案 |
|--------|-----------|------|----------|
| METADATA_NOT_FOUND | 400 | 元数据未找到 | 1. 检查表名是否正确<br>2. 调用 /metadata/reload 刷新元数据 |
| UNSUPPORTED_SYNTAX | 422 | 不支持的SQL语法 | 1. 简化SQL语句<br>2. 拆分为多个简单语句<br>3. 查看文档了解支持的语法 |
| INTERNAL_PARSE_ERROR | 500 | 内部解析错误 | 联系技术支持并提供完整SQL脚本 |

### 5.2 HTTP错误码

| HTTP状态码 | 说明 | 常见原因 |
|-----------|------|----------|
| 400 | Bad Request | 参数错误、元数据未找到 |
| 413 | Payload Too Large | 文件大小超过限制（10MB） |
| 422 | Unprocessable Entity | SQL语法不支持 |
| 500 | Internal Server Error | 系统内部错误 |

### 5.3 错误响应格式

```json
{
  "status": "400",
  "data": null,
  "message": "元数据缺失: 未找到表的元数据信息: test_db.non_existent_table\n建议：1) 检查表名是否正确 2) 调用 /metadata/reload 刷新元数据"
}
```

---

## 6. 使用场景示例

### 场景1：数据影响分析

**需求**：修改 `users` 表结构前，需要知道哪些下游表会受影响。

**步骤**：
1. 查询表的下游依赖
```bash
curl "http://localhost:8080/sql/lineage/table/downstream?schema=test_db&tableName=users&depth=3"
```

2. 查询特定列的下游依赖
```bash
curl "http://localhost:8080/sql/lineage/column/downstream?schema=test_db&tableName=users&columnName=user_id&depth=3"
```

---

### 场景2：数据溯源

**需求**：`final_report` 表中的 `total_amount` 字段数据异常，需要追溯数据来源。

**步骤**：
1. 查询列的上游依赖
```bash
curl "http://localhost:8080/sql/lineage/column/upstream?schema=test_db&tableName=final_report&columnName=total_amount&depth=5"
```

2. 分析返回的血缘链路，定位源头表和列

---

### 场景3：批量SQL脚本解析

**需求**：解析ETL脚本，生成完整的数据流图。

**步骤**：
1. 准备SQL脚本文件 `etl_script.sql`
2. 上传解析
```bash
curl -X POST http://localhost:8080/sql/analyzer/upload \
  -F "file=@etl_script.sql"
```

3. 将返回的 `LineageGraph` 导入可视化工具

---

## 7. 最佳实践

### 7.1 性能优化

1. **控制查询深度**：从 `depth=1` 开始，逐步增加
2. **定期刷新元数据**：配置合理的 `refresh-cron`
3. **批量解析**：合并多个SQL文件后一次性解析

### 7.2 错误处理

1. **捕获异常**：所有接口都可能返回错误，需要处理 `status != "200"` 的情况
2. **查看警告**：即使解析成功，也要检查 `warnings` 字段
3. **日志追踪**：使用 `traceId` 追踪问题

### 7.3 SQL编写建议

1. **显式列名**：避免使用 `SELECT *`，明确指定列名
2. **使用别名**：为表和列提供清晰的别名
3. **避免动态SQL**：使用静态SQL便于解析

---

## 8. 附录

### 8.1 支持的SQL方言特征

| 方言 | 检测特征 | 示例 |
|------|---------|------|
| ClickHouse | `ENGINE =`, `PREWHERE`, `FINAL` | `CREATE TABLE t ENGINE = MergeTree()` |
| PostgreSQL | `::`, `RETURNING`, `LATERAL` | `SELECT id::INTEGER` |
| Oracle | `DUAL`, `ROWNUM`, `CONNECT BY` | `SELECT * FROM DUAL` |
| SQL Server | `TOP`, `[dbo]`, `WITH (NOLOCK)` | `SELECT TOP 10 * FROM t` |
| ODPS | `DISTRIBUTE BY`, `INSERT OVERWRITE` | `INSERT OVERWRITE TABLE t` |

### 8.2 常用Curl命令模板

```bash
# 解析SQL文本
curl -X POST http://localhost:8080/sql/analyzer/parse \
  -H "Content-Type: text/plain" \
  -d @query.sql

# 上传文件
curl -X POST http://localhost:8080/sql/analyzer/upload \
  -F "file=@script.sql"

# 查询上游
curl "http://localhost:8080/sql/lineage/table/upstream?schema=db&tableName=t&depth=2"

# 刷新元数据
curl -X POST http://localhost:8080/sql/analyzer/metadata/reload

# 查看统计
curl http://localhost:8080/sql/analyzer/metadata/stats
```

---

**文档版本**：v1.0
**最后更新**：2024-11-11
**维护者**：afsun
