# SQL血缘分析系统 - 前端部署指南

## 快速启动

### 1. 安装依赖

进入前端目录并安装依赖：

```bash
cd sql-Lineager-enhance/frontend
npm install
```

如果安装速度慢，可以使用国内镜像：

```bash
npm install --registry=https://registry.npmmirror.com
```

### 2. 启动后端服务

确保后端服务已启动（默认端口8080）：

```bash
cd sql-Lineager-enhance
mvn spring-boot:run
```

### 3. 启动前端开发服务器

```bash
cd frontend
npm run dev
```

访问 `http://localhost:3000` 即可使用系统。

## 功能演示

### 页面1：SQL解析

![SQL解析页面](docs/images/sql-parser.png)

**功能说明**：
- 输入SQL语句或上传SQL文件
- 实时解析并展示血缘关系
- 支持三种视图：图形、表格、JSON

**操作步骤**：
1. 在文本框输入SQL或点击"上传SQL文件"
2. 点击"解析SQL"按钮
3. 查看解析结果和血缘关系图

**示例SQL**：
```sql
-- 示例1：简单的INSERT SELECT
INSERT INTO test_db.target_table(id, name, age)
SELECT user_id, user_name, user_age
FROM test_db.source_table;

-- 示例2：CTAS（Create Table As Select）
CREATE TABLE test_db.user_summary AS
SELECT
    u.user_id,
    u.user_name,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_amount
FROM test_db.users u
LEFT JOIN test_db.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.user_name;

-- 示例3：WITH子句（CTE）
WITH active_users AS (
    SELECT user_id, user_name
    FROM test_db.users
    WHERE status = 'active'
)
INSERT INTO test_db.active_user_list
SELECT * FROM active_users;
```

### 页面2：血缘查询

![血缘查询页面](docs/images/lineage-query.png)

**功能说明**：
- 查询表的上下游依赖关系
- 查询列的上下游依赖关系
- 查询两个表之间的血缘路径
- 支持多层级深度查询（1-10层）

**操作步骤**：
1. 选择查询类型（表上游/下游、列上游/下游、血缘路径）
2. 填写源对象信息
   - 模式/Schema：必填（如：test_db）
   - 表名：必填（如：user_summary）
   - 列名：列级查询时必填（如：order_count）
3. 设置查询深度（建议从1开始）
4. 点击"查询"按钮
5. 查看结果：表格视图或可视化图形

**查询示例**：

**示例1：查询表的上游依赖**
- 查询类型：表上游依赖
- 模式：test_db
- 表名：user_summary
- 深度：2

结果：显示 user_summary 表的数据来源于哪些表

**示例2：查询列的血缘**
- 查询类型：列上游依赖
- 模式：test_db
- 表名：user_summary
- 列名：order_count
- 深度：1

结果：显示 order_count 列的数据来源于哪些列

**示例3：查询血缘路径**
- 源表：test_db.users
- 目标表：test_db.final_report

结果：显示从 users 到 final_report 的所有血缘传递路径

### 页面3：元数据管理

![元数据管理页面](docs/images/metadata-management.png)

**功能说明**：
- 查看元数据统计信息
- 手动刷新元数据缓存
- 查看刷新历史记录
- 监控刷新性能趋势
- 导出统计数据

**操作步骤**：
1. 查看当前元数据统计（总表数、总列数、缓存大小）
2. 点击"刷新元数据"按钮手动刷新
3. 查看刷新历史和性能图表
4. 点击"导出统计"下载数据

**注意事项**：
- 元数据会在应用启动时自动加载
- 系统会定时刷新元数据（默认每小时一次）
- 手动刷新会重新加载所有非系统库的表和列信息
- 刷新过程不影响正在进行的SQL解析任务

## 可视化图形操作指南

### 图形工具栏

![图形工具栏](docs/images/graph-toolbar.png)

- **放大**：点击放大按钮或使用鼠标滚轮向上滚动
- **缩小**：点击缩小按钮或使用鼠标滚轮向下滚动
- **重置**：恢复默认视图，自动适应画布大小
- **布局切换**：
  - **层级布局**：自上而下的树形结构，适合查看数据流向
  - **力导向**：自动优化节点位置，适合复杂关系
  - **网格布局**：规则的网格排列，适合整齐展示
- **显示切换**：
  - **显示列级**：展示表和列的完整血缘关系
  - **仅表级**：只展示表之间的依赖关系（性能更好）

### 图形交互

- **拖拽画布**：按住鼠标左键拖动
- **拖拽节点**：点击节点并拖动到新位置
- **缩放**：使用鼠标滚轮
- **点击节点**：查看节点详细信息
- **悬停节点**：显示节点提示信息

### 节点说明

- **蓝色节点**：表节点
- **橙色节点**：列节点
- **实线箭头**：列到列的血缘关系（数据流向）
- **虚线箭头**：列属于表的关系

## 常见使用场景

### 场景1：数据影响分析

**需求**：修改 users 表结构前，需要知道哪些下游表会受影响。

**操作步骤**：
1. 进入"血缘查询"页面
2. 选择"表下游依赖"
3. 填写：
   - 模式：test_db
   - 表名：users
   - 深度：3（查询3层下游）
4. 点击"查询"
5. 查看结果：
   - 表节点列表显示所有受影响的表
   - 可视化图形展示完整的影响链路

**进一步分析**：
- 如果要查看某个具体列的影响，切换到"列下游依赖"
- 填写列名（如：user_id）查看该列的使用情况

### 场景2：数据溯源

**需求**：final_report 表中的 total_amount 字段数据异常，需要追溯数据来源。

**操作步骤**：
1. 进入"血缘查询"页面
2. 选择"列上游依赖"
3. 填写：
   - 模式：test_db
   - 表名：final_report
   - 列名：total_amount
   - 深度：5（深度追溯）
4. 点击"查询"
5. 分析结果：
   - 查看列节点列表，按层级排序
   - 找到最源头的数据表和列
   - 在可视化图形中查看完整的数据流转路径

### 场景3：批量SQL脚本解析

**需求**：解析ETL脚本，生成完整的数据流图。

**操作步骤**：
1. 准备SQL脚本文件（如：etl_script.sql）
2. 进入"SQL解析"页面
3. 点击"上传SQL文件"，选择文件
4. 系统自动解析并展示结果
5. 切换到"图形视图"查看完整的血缘关系图
6. 使用工具栏调整布局和显示级别
7. 点击节点查看详细信息

**提示**：
- 大型脚本建议使用"仅表级"模式，性能更好
- 可以导出JSON视图保存解析结果
- 支持多语句SQL脚本

### 场景4：血缘路径分析

**需求**：了解从源表到目标表的数据传递路径。

**操作步骤**：
1. 进入"血缘查询"页面
2. 选择"血缘路径"
3. 填写源表信息：
   - 模式：test_db
   - 表名：users
4. 填写目标表信息：
   - 模式：test_db
   - 表名：final_report
5. 点击"查询"
6. 查看结果：
   - 系统会列出所有可能的路径
   - 每条路径显示完整的表传递链
   - 可以对比不同路径的长度和复杂度

## 性能优化建议

### 1. SQL解析优化

- **大型SQL脚本**：
  - 建议拆分为多个小文件（每个文件<1000行）
  - 避免在一个脚本中包含过多复杂查询

- **图形渲染**：
  - 节点数>100时，使用"仅表级"模式
  - 使用"层级布局"而非"力导向"（性能更好）

### 2. 血缘查询优化

- **深度控制**：
  - 首次查询使用 depth=1
  - 根据需要逐步增加深度
  - depth>5 时查询可能较慢

- **查询范围**：
  - 优先使用表级查询（比列级查询快）
  - 列级查询时明确指定列名

### 3. 浏览器性能

- **推荐浏览器**：Chrome、Edge（性能最佳）
- **定期清理**：清理浏览器缓存和Cookie
- **关闭无关标签页**：减少内存占用

## 故障排查

### 问题1：页面无法访问

**症状**：浏览器显示"无法访问此网站"

**排查步骤**：
1. 检查前端服务是否启动：
   ```bash
   # 查看进程
   netstat -ano | findstr "3000"
   ```
2. 检查端口是否被占用：
   ```bash
   # Windows
   netstat -ano | findstr "3000"
   # 如果被占用，修改 vite.config.js 中的端口
   ```
3. 查看控制台错误信息

### 问题2：API请求失败

**症状**：页面显示"请求失败"或"网络错误"

**排查步骤**：
1. 检查后端服务是否启动：
   ```bash
   # 访问后端健康检查接口
   curl http://localhost:8080/actuator/health
   ```
2. 检查代理配置：
   - 打开 `vite.config.js`
   - 确认 `proxy.target` 指向正确的后端地址
3. 查看浏览器控制台Network标签：
   - 查看请求URL是否正确
   - 查看响应状态码和错误信息

### 问题3：图形不显示

**症状**：解析成功但图形区域空白

**排查步骤**：
1. 打开浏览器控制台，查看是否有JavaScript错误
2. 检查解析结果是否为空：
   - 切换到"JSON视图"查看数据
   - 确认 `graph.tables` 和 `graph.columns` 不为空
3. 尝试刷新页面
4. 尝试切换布局模式

### 问题4：元数据刷新失败

**症状**：点击"刷新元数据"后显示失败

**排查步骤**：
1. 检查ClickHouse连接：
   ```bash
   # 测试连接
   curl http://localhost:8123/
   ```
2. 检查后端日志：
   ```bash
   # 查看日志文件或控制台输出
   tail -f logs/application.log
   ```
3. 检查数据库配置：
   - 打开 `application.yml`
   - 确认数据源配置正确

### 问题5：文件上传失败

**症状**：上传SQL文件后显示错误

**排查步骤**：
1. 检查文件大小（限制10MB）
2. 检查文件编码（必须是UTF-8）
3. 检查文件格式（.sql或.txt）
4. 尝试复制文件内容到文本框直接解析

## 开发调试

### 启用调试模式

在 `src/main.js` 中添加：

```javascript
// 开发环境下启用Vue DevTools
if (process.env.NODE_ENV === 'development') {
  app.config.performance = true
  app.config.devtools = true
}
```

### 查看API请求

在 `src/api/index.js` 的请求拦截器中添加日志：

```javascript
service.interceptors.request.use(
  config => {
    console.log('API Request:', config.url, config.data)
    return config
  }
)
```

### 调试图形渲染

在 `LineageGraph.vue` 中添加：

```javascript
const buildGraphData = () => {
  const { nodes, edges } = buildGraphData()
  console.log('Graph Nodes:', nodes)
  console.log('Graph Edges:', edges)
  // ...
}
```

## 生产部署

### 构建生产版本

```bash
npm run build
```

构建产物在 `dist` 目录。

### Nginx部署

1. 将 `dist` 目录内容复制到Nginx的html目录
2. 配置Nginx：

```nginx
server {
    listen 80;
    server_name your-domain.com;

    root /usr/share/nginx/html;
    index index.html;

    # 前端路由
    location / {
        try_files $uri $uri/ /index.html;
    }

    # API代理
    location /api {
        proxy_pass http://backend-server:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    # 静态资源缓存
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg)$ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

3. 重启Nginx：
```bash
nginx -s reload
```

### Docker部署

创建 `Dockerfile`：

```dockerfile
FROM node:16-alpine as builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY src .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

构建和运行：

```bash
docker build -t sql-lineage-ui .
docker run -d -p 80:80 sql-lineage-ui
```

## 更新日志

### v1.0.0 (2024-11-11)

**新增功能**：
- ✅ SQL解析页面（文本输入、文件上传、多视图展示）
- ✅ 血缘查询页面（表级/列级查询、路径查询）
- ✅ 元数据管理页面（统计、刷新、监控）
- ✅ 可视化血缘关系图（多布局、交互式）
- ✅ 响应式设计（支持移动端）

**技术特性**：
- Vue 3 Composition API
- Element Plus UI组件库
- ECharts数据可视化
- vis-network网络图
- Axios HTTP客户端
- Vite构建工具

## 联系支持

- 技术文档：查看 `README.md`
- API文档：查看 `API_DOCUMENTATION.md`
- 问题反馈：[GitHub Issues]
- 作者：afsun
