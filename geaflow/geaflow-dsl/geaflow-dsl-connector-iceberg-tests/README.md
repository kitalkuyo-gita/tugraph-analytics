# Iceberg 多进程生产验收测试

此模块只在 `iceberg-cluster-it` profile 中启用，要求 JDK 17。它加载完整 GeaFlow runtime 和最终 `isolated` connector JAR，宿主依赖排在 connector 前面。

## 集群与故障边界

每个场景启动独立 client、master、driver 和四个 container JVM；每个 container 只有一个 worker，三个 source task 分布于三个进程。正常场景的 ALL 查询还执行全局排序和 shuffle。

`ProcessClusterManager` 仅提供测试环境中的进程启动/重启；RPC、调度、心跳、`cluster_fo`、RocksDB checkpoint、driver pipeline 恢复、JDBC offset 和 Iceberg 计划恢复使用项目实际代码。测试关闭 `geaflow.run.local.mode`，不使用不支持进程恢复的 `LocalClusterManager`。supervisor 在引擎触发 master 退出后重启 master 一次，模拟部署平台的进程托管。

这验证同一主机上的真实多进程恢复，不能代替 Ray/Kubernetes、多主机网络故障、远端 checkpoint 存储或生产认证的部署验收。

## 场景与断言

- `all`：ALL + 过滤 + 全局排序/shuffle + sink。
- `size`：SIZE=10 + 过滤 + 多窗口 sink。
- `size-kill`：flying=1，窗口 3 的 sink 已写入三行但事务尚未提交时，向拥有该 worker 的进程发送 SIGKILL。
- `flying-kill`：同一故障点，flying=3，验证多个飞行批次的恢复。
- `graph`：Iceberg 数据写入 RocksDB 图，再经 MATCH 查询输出。

输入为 12 个真实 Parquet 数据文件、position/equality delete 文件和两条重复业务行。SQL 场景比较完整的 475 行多重集，不能使用去重后的集合掩盖重复。图场景按 vertex ID 合并重复输入后比较 473 个点。

故障前要求已有已提交窗口和持久 source offset。故障后追加新 snapshot，要求恢复采用完全相同的计划根记录、排除新数据，并由不同 PID 重放同一 window/task。检查 master 非正常退出与恢复、driver 从持久上下文重建 pipeline、三个进程实际执行 source，而不是只检查配置中的并行度。

测试 sink 按 `(window, task)` 事务替换输出，在 kill 时回滚未提交事务，重放时覆盖相同窗口，保留窗口内真实重复行。这是测试的幂等消费契约，**不构成任意生产 sink 的 exactly-once 保证**。

## 执行

准备可创建 schema 的专用 PostgreSQL 17（用户 postgres）和专用 Redis 7。不要使用生产数据库/Redis；PG 使用每次运行独立 schema，Redis 使用唯一 job 身份，测试没有删除 Redis 中的历史注册键。密码通过 `ICEBERG_TEST_PG_PASSWORD` 环境变量提供，可省略以使用本地 trust fixture。

```bash
mvn -B -ntp -Piceberg-jdk17,iceberg-cluster-it \
  -pl geaflow/geaflow-dsl/geaflow-dsl-connector-iceberg-tests -am install \
  -DskipTests -Dskip.yarn=true -Dmaven.javadoc.skip=true

mvn -B -ntp -Piceberg-cluster-it \
  -pl geaflow/geaflow-dsl/geaflow-dsl-connector-iceberg-tests verify \
  '-Diceberg.test.postgres.url=jdbc:postgresql://localhost:5432/postgres?user=postgres' \
  -Diceberg.test.redis.port=6379 -Dmaven.javadoc.skip=true
```

可用 `-Diceberg.test.scenario=size-kill` 单独定位一个场景。缺少服务配置直接失败，不静默跳过。`skip.yarn=true` 仅省略无关的 dashboard 前端构建，不代表发行包验收。

每个子 JVM 堆上限 512 MiB，测试父 JVM 1 GiB；按约 5 GiB 可用内存及本地 RPC 端口预算运行。默认依次运行场景，不并发竞争测试集群资源。

## 验证记录

`target/failsafe-reports` 保存测试结果。`target/cluster-<scenario>-<unique>/` 保留：

- `acceptance.txt`：仅所有断言通过后生成；场景、进程数、窗口并发、输出行数。
- `*.log`、`*.initial-pid`、`*.recovered-pid`：角色日志及重启前后 PID。
- `before-kill.txt`、`database-evidence.txt`：故障标记、window/task/PID 执行审计、窗口输出行数、持久 offset 数。
- `plan-before.json`、`plan-after.json`：恢复前后固定计划的精确比较。
- `query.sql`、warehouse、checkpoint 和工作目录：本地故障诊断材料。

测试结束关闭其创建的进程并删除自己的 PG schema，保留 target 下的文件供诊断。CI 上传日志、验收摘要、数据库证据、计划及测试报告；不上传完整 warehouse/checkpoint 或配置凭证文件。失败日志中的连接错误应结合 kill、master 重启和最终断言判断。
