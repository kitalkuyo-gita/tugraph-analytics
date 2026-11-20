# Iceberg connector P0/P1 实现与验证记录

日期：2026-11-20。对应设计：[开发落地方案](geaflow-iceberg-connector-development-plan.md)。

## 1. 本次交付结论

已增加可选择构建的 `geaflow-dsl-connector-iceberg`，先通过 P0 SDK、序列化、持久化恢复验证，再实现并验证 P1 固定快照读取。当前定位是具有明确能力边界的可集成实现；本记录列出的本地验证通过，不等于设计方案中全部生产环境准入项已经完成。

首轮 P0/P1 实现通过 **22 项测试**：Iceberg 单元/功能测试 17 项、制品与服务集成测试 3 项、公共恢复基础设施回归测试 2 项。本轮推进后累计覆盖 **37 个不同用例，全部通过**；多进程生产验收、缺陷修复与追加回归见第 7 节。测试使用真实 Iceberg 元数据和 Parquet 文件，不用 mock reader 代替实际删除语义。

入口与运行说明：[模块 README](../geaflow/geaflow-dsl/geaflow-dsl-connector/geaflow-dsl-connector-iceberg/README.md)。

## 2. P0：兼容性与恢复基础

### 2.1 版本和隔离边界

| 项目 | 本次选择与验证 |
| --- | --- |
| SDK | Apache Iceberg 1.12.0；运行时检查版本，状态记录 SDK 版本 |
| Java | Temurin 17.0.21；模块 enforcer 限定 JDK 17 |
| 构建 | Maven 3.9.11；`iceberg-jdk17` 显式 profile |
| 默认构建 | 不启用 profile 就不加入 Iceberg 模块，不修改默认 JDK 8/11 目标 |
| 发布制品 | `geaflow-dsl-connector-iceberg-0.8.0-SNAPSHOT-isolated.jar` |
| 私有依赖 | relocation：Iceberg、Parquet、Avro、Jackson、Commons、HTTP client 5 等 |
| 共享依赖 | GeaFlow API、宿主 Hadoop 2.7.4、SLF4J 和 native codec 包 |
| 混合验证 | 独立 JVM 同时加载默认 file/Paimon 运行时及 isolated Iceberg；旧 Parquet 1.10.1 读取文件 footer，新 Iceberg 读取正确可见行 |
| 压缩与 catalog | isolated 制品验证 Snappy/Gzip/Zstd，HadoopCatalog、本地 HTTP REST catalog 协议 |

本次下载的 `iceberg-core-1.12.0.jar` SHA-256：`d3bbb331b7a907e134958d21ecccd91a736049e00768e07579ff60855bf28352`；SDK 报告源码提交 `212aa894e919ff9fdf89160da4aadcbe6858ac01`。

开发中实际发现并修复两类打包问题：过宽 artifact 排除规则会误删 connector 自身；将 Hadoop 全量 relocation 会导致 XML 配置的 group provider 与接口类不一致。因此采用明确的宿主 Hadoop 边界，并用最终 JAR 验证。不能把这个 JAR 描述成所有依赖完全自包含。

`IcebergTaskReader` 使用 1.12.0 公开的 `FormatModelRegistry` / `ReadBuilder`、`GenericDeleteFilter`，没有复制 Iceberg 私有实现，也没有将 connector 类伪装放入 Iceberg package。

### 2.2 持久化计划与游标

一次扫描由稳定 `run-id` 标识。第一次 open 解析 selector、冻结 snapshot/schema/field IDs，生成 task JSON 后，将根记录和全部 tasks 放在同一个 JDBC 事务中提交。并发规划由主键选出唯一提交者，其他调用采用已提交计划。

根记录包含 table UUID、nullable snapshot ID、schema/projection JSON、name mapping、SDK/codec 版本、固定 shard 数及校验值。空表用 null snapshot 表示，不占用任意合法的 signed long snapshot ID。

任务使用 SDK `ScanTaskParser` JSON，不把 `Table`、Catalog、FileIO、迭代器或 SDK task 对象直接放入引擎恢复状态。task 身份基于 table UUID、snapshot、spec、原始文件 URI、start/length/content，排序后确定 shard。查询时验证根校验值、任务顺序、任务数量和 shard 摘要。

游标是不可变 DTO，包含版本、plan checksum、shard、task index/key、删除过滤后的行序号、输出行计数和目标 window ID。支持显式二进制编解码，Java/Kryo 只携带可序列化状态。恢复时从原 task 重读并跳过已确认的可见行，因此速度可能与单 task 内恢复位置成正比。

以下情况明确失败，不退回 latest：计划或 task 缺失/损坏、snapshot 被清理、UUID 变化、参数或投影不一致、游标版本错误、window ID 不一致。重建执行图时设置 `resume=true`，防止丢失计划被误认为第一次运行。

### 2.3 公共恢复基础设施修复

| 修改 | 原问题与结果 |
| --- | --- |
| `OffsetStore.writeOffset` | 持久化前先失效缓存，两个 store 写入成功后才更新缓存；异常后重新读取权威存储，避免将未确认进度留在缓存 |
| `OffsetStore.close` | 实现 AutoCloseable，释放两个 store；一个关闭异常也继续关闭另一个 |
| `GeaFlowTableSourceFunction.close` | 用 finally 链关闭 source、offset store、分区检查线程池 |
| `BaseJdbcStore` | 静态连接池改为实例所有，避免跨作业复用错误数据库/凭证及一方关闭影响另一方；关闭幂等 |

连接池变更有实际容量影响：每个 JDBC store 独立池，单个 OffsetStore 拥有两个 store。`geaflow.store.jdbc.connection.pool.size` 默认 10 是**每池**上限，部署前需要按 source 实例数核算连接数。计划库使用短连接，不占用这些 OffsetStore 池。

这不增加端到端 exactly-once 承诺。下游处理后、offset 保存前失败仍可能重放；下游必须接受该交付语义。

## 3. P1：固定快照读取

| 能力 | 当前实现 |
| --- | --- |
| SPI | `iceberg` readable table；没有 sink |
| Catalog | Hadoop / REST，禁止自定义 `catalog-impl`、`io-impl` 绕过当前边界 |
| Selector | 首次规划时 latest、snapshot-id、as-of-timestamp-ms、ref；互斥；之后固定 |
| 表/文件 | format v1/v2，未加密 Parquet data 和 delete 文件 |
| 删除 | position/equality delete，使用 SDK sequence 语义；补读投影外 equality key |
| Schema | 按冻结 field ID 读取，按 DSL 列顺序返回；支持 rename 后恢复 |
| 类型 | BOOLEAN、INTEGER、LONG、FLOAT、DOUBLE、STRING、BINARY_STRING、DECIMAL、DATE |
| 分区 | 固定 K 个逻辑 shard，与 worker P 无关；由已有 KeyGroup 分配 |
| ALL | 惰性跨 task 迭代；读取完成后才由 SourceFunction 保存 END |
| SIZE | 按 shard 输出行数计数，跨 task，受估算字节预算限制可提前结束窗口 |
| 重试 | 传入持久游标为权威；缓存仅在游标完全吻合时复用；中途放弃 ALL 后可在同实例重放，旧迭代器失效 |
| 资源 | 异常关闭 reader，source close 幂等；恢复保留根状态，重建 Catalog/FileIO |

ALL END 的控制台行计数为 `-1`（惰性读取返回游标时最终数量未知），该数值不是恢复状态。TIME window 显式拒绝。

尚不支持：v3/v4、删除向量/row lineage/新类型、Avro/ORC、加密文件、CDC/增量、sink、在线 rescale、复杂类型和 TIMESTAMP。TIMESTAMP 暂缓是因为现有 GeaFlow 类型序列化精度限制；历史 equality-delete key 已从冻结 snapshot schema 删除的表也会明确拒绝，后续需要历史 schema union。

## 4. 验证证据与覆盖

| 测试 | 方法数 | 已验证内容 |
| --- | ---: | --- |
| `IcebergCompatibilityTest` | 2 | 多 row group 的真实 position/equality delete；task JSON/Kryo；独立 JVM 根据磁盘配置和二进制游标恢复，排除后来 append |
| `IcebergTableSourceTest` | 7 | SPI；K=1/7/64 与 P=1/4/8 分配；删除后重新插入的可见行 multiset；快照/rename/序列化恢复；SIZE/ALL 重放；空表；selector；失败关闭 |
| `IcebergEngineRecoveryTest` | 1 | 真实 SourceFunction.fetch + JDBC OffsetStore；collect 注入失败不提前提交；重建 source/store 继续；offset 环形槽位 window 冲突检测 |
| `IcebergValidationTest` | 6 | v1、分区演进、null equality、decimal(38)、早于 epoch 的 DATE、Unicode/BinaryString/NaN；内存阈值；codec/游标损坏；UUID 变化；配置与类型拒绝 |
| `IcebergRestCatalogTest` | 1 | SDK REST 客户端经真实 HTTP 读取本地服务配置和表元数据 |
| `IcebergIsolatedIT` | 1 | 真正 isolated JAR 的类加载、SPI、Hadoop/REST、删除和压缩编码 |
| `PostgresPlanStoreIT` | 1 | PostgreSQL 17.12 并发发布只采用一个根；task 插入故障导致根/tasks 整体回滚，恢复后可重新提交 |
| `IcebergMixedRuntimeIT` | 1 | 独立 JVM，260 个宿主 runtime JAR 与 isolated connector；无原始 Iceberg；file/Paimon/Iceberg SPI 共存与真实文件读取 |
| `OffsetStoreRecoveryTest` | 1 | 持久化失败后的缓存失效与重读 |
| `JdbcPoolIsolationTest` | 1 | 不同数据库实例隔离，一方关闭后另一方继续工作 |
| **总计** | **22** | **全部通过，无跳过** |

Checkstyle、Apache RAT 和差异空白检查已通过。PostgreSQL 测试容器使用本机回环端口，完成后已停止并移除。未更改用户的数据库或持久化服务。

需要正确理解证据范围：SourceFunction 测试接通真实 fetch/store 路径，但不是完整集群调度；独立 JVM 恢复验证了无父进程对象依赖；首轮未覆盖的真实 worker kill 与调度重建现已在第 7 节补充。REST fixture 是最小本地无认证服务，不是生产 REST/OAuth/S3 认证。

## 5. 构建与测试复现

在 JDK 17、Maven、项目所需 protoc 可用的环境，从仓库根目录执行。

先验证 P0：

```bash
mvn -B -ntp -Piceberg-jdk17 \
  -pl geaflow/geaflow-dsl/geaflow-dsl-connector/geaflow-dsl-connector-iceberg -am test \
  '-Dtest=IcebergCompatibilityTest,OffsetStoreRecoveryTest,JdbcPoolIsolationTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true
```

完整 P1 与 isolated 制品验证，并安装后续混合 classpath 检查所需依赖：

```bash
mvn -B -ntp -Piceberg-jdk17 \
  -pl geaflow/geaflow-dsl/geaflow-dsl-connector/geaflow-dsl-connector-iceberg -am install \
  '-Dtest=Iceberg*Test,OffsetStoreRecoveryTest,JdbcPoolIsolationTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true
bash tools/ci/check-iceberg-isolation.sh
```

使用可创建测试 schema 的专用 PostgreSQL 数据库时，在前一条 Maven 命令中使用 profile `iceberg-jdk17,iceberg-postgres-it`，并加入 `'-Diceberg.test.postgres.url=jdbc:postgresql://localhost:5432/postgres?user=postgres'`。密码通过 `ICEBERG_TEST_PG_PASSWORD` 环境变量提供。测试仅操作随机生成的独立 schema。

新工作流 [ci-iceberg-jdk17.yml](../.github/workflows/ci-iceberg-jdk17.yml) 配置了 JDK 17、PostgreSQL service、全部模块测试和混合依赖检查；隔离检查 shell 脚本已在本地实际执行成功。本次没有触发远程 GitHub Actions。

发行包的 opt-in 构建入口：

```bash
mvn -B -ntp -Piceberg-jdk17 \
  -pl geaflow/geaflow-deploy/geaflow-assembly -am package \
  -DskipTests -Dmaven.javadoc.skip=true
```

本次也尝试了上述完整 assembly 构建：相关 Java 模块已推进到 dashboard，随后持续停留在既有前端 `yarn install` 下载阶段，已主动停止。**完整发行包未验收通过**；不能把 isolated JAR 和混合 JVM 测试的成功等同于最终 tar 包成功。需在前端依赖可用的环境补跑该命令。

随后增加 `-Dskip.yarn=true` 验证 Java assembly：**87 个 reactor 模块构建成功**，生成约 312 MiB shaded JAR，并检查到 Iceberg SPI、隔离后的 `Table.class`，未混入未隔离的 `org.apache.iceberg.Table`。此项证明 Java 打包集成路径可用；它跳过了前端资源构建，不能作为包含 dashboard 的完整发行验收。最后的凭证属性校验补丁另经模块完整测试及 isolated JAR 测试复验，正式发布应从最终源码重建全部制品。

## 6. 相对原方案的取舍与上线前剩余工作

当前配置契约以模块 README 为准，原方案第 14 节是拟议配置，不能直接复制执行。P1 采用扁平键：`read.mode` 对应 `mode`，`scan.snapshot-id/ref/as-of-timestamp-ms` 对应 `snapshot-id/ref/as-of-timestamp-ms`，`source.shard-count` 对应 `shard-count`，`scan.run-id` 对应 `run-id`，恢复使用 `resume=true`。Catalog 属性使用 `catalog.properties.*`，secret 使用环境变量引用，计划库使用 `plan.jdbc.*`。未提供旧拟议键的别名，未知键明确报错。

表名采用 SDK `TableIdentifier.parse` 的点分 identifier，尚未实现原方案的 `namespace.json` 独立表示；含字面点号的单个 namespace/table 名称不在当前支持范围。Hadoop 配置由宿主 Configuration 提供，未增加 JSON 透传或自定义 FileIO 加载器。后续扩展配置必须先明确与现有 fingerprint 的兼容策略。

1. **计划库采用有界内存实现。** 默认最多 100,000 tasks、64 MiB 序列化任务估算字节；规划和单 shard 加载仍物化。未实现无限规模外部排序/流式分页。manifest 和展开后的 delete index 内存需要额外预算，不能把压缩文件大小等同于 JVM 内存上限。
2. **计划恢复与 offset 恢复分别配置。** 建表 SQL 在模块资源目录。集群使用所有 worker 可访问的持久计划库及持久 offset backend；MEMORY offset backend 无法跨进程恢复。run-id 保持稳定，重建图使用 resume=true，snapshot/data/delete/计划/offset 共同保留至恢复窗口结束。引擎 offset key 还包含 job/table 身份，恢复时也必须保留；新建 job 仅复用 run-id 会重用快照计划，不会自动导入旧 job 的游标。
3. **宿主依赖保留有意边界。** Hadoop/native codecs 与原发行包共享；当前通过了本地文件、REST 协议、混合 classpath 验证，目标 HDFS/S3、安全配置和续期仍需独立验收。
4. **没有扩大到 P2。** 不以 append 扫描冒充 CDC，不新增 SQL filter pushdown，也不实现写入。
5. **故障测试仍有后续项。** 已覆盖任务插入事务失败、collect 失败、损坏状态、进程恢复；第 7 节补齐了 worker kill、飞行批次和协调器恢复；仍未故障注入 commit 成功但 ACK 丢失、数据库主从切换。代码对 commit 异常按 run key 重新查证，但不能据此声称 lost-ACK 已验收。
6. **规模与环境准入待完成。** 百 GB/TB、长时稳定性、目标部署平台的多主机调度、复杂生产图查询、JDK 8/11 默认发行版回归及认证对象存储矩阵仍待运行。第 7 节覆盖同机多进程的 SQL/图查询与恢复。生产开放前应按原方案补齐。

上述实现与验收阶段未创建提交或推送；工作区中原有文档未被覆盖。


## 7. P1 生产验收推进：真实多进程与端到端（2026-11-20）

### 7.1 为什么增加独立验收模块

新增 `geaflow-dsl-connector-iceberg-tests`，仅在 `iceberg-cluster-it` profile 中启用。详见[执行说明与证据格式](../geaflow/geaflow-dsl/geaflow-dsl-connector-iceberg-tests/README.md)。它依赖完整 DSL runtime 和最终 isolated JAR，而不是 connector 的未隔离源码 classpath。

原生 LOCAL 部署不能用作此次恢复验收：`LocalClusterManager` 的组件位于同一 JVM，restart 方法没有实际重启；`LocalFailoverStrategy` 也不提供目标恢复行为。因此测试提供最小进程部署适配器，独立启动 client、master、driver 和四个单 worker container，关闭 `geaflow.run.local.mode`，使用真实 RPC、资源分配、Redis HA、`cluster_fo`、RocksDB checkpoint 和 PostgreSQL offset/plan store。三个 source task 确实在三个不同 container 进程中打开，测试直接检查日志和 PID。

故障由事务中的 sink 屏障精准定位：窗口 3 已写入三行但未提交，父进程先确认此前窗口已提交、持久 offset 非空，再 SIGKILL 拥有该 worker 的进程。心跳/通信失败由引擎触发 master 退出；测试 supervisor 只负责重启 master 一次，随后由实际 `ClusterFailoverStrategy` 重启 driver/containers、加载持久 pipeline 和 checkpoint。没有用测试代码直接调用 source.fetch 或手工拼装恢复游标来替代调度恢复。

这里的“真实多 worker”指同机独立 OS 进程与真实引擎通信/恢复；不是 Ray/Kubernetes 或跨机器部署认证。本地 warehouse/checkpoint 由各 JVM 共同访问，生产远端文件系统和网络分区仍须单独验收。

### 7.2 实际发现并修复的问题

| 问题 | 触发条件 / 修复 | 回归证据 |
| --- | --- | --- |
| Parquet 内嵌 Thrift 冲突 | 完整宿主 classpath 优先时，`shaded.parquet...TIOStreamTransport.getConfiguration()` 产生 `NoSuchMethodError`。原 relocation 只覆盖 `org.apache.parquet`，漏掉 Parquet 自带的 `shaded.parquet`；补充该命名空间 relocation | mixed runtime 现在分别运行宿主优先、插件优先；集群测试使用宿主优先的 isolated JAR |
| source 并行度配置失效 | `getConfigParallelisms(opName, sourceParallelism) == -1` 将计算出的默认值误当成显式配置，导致配置 3 个 source 实际仅 1 个。改用 `-1` 判断是否有显式算子配置，保留 table/operator 配置优先级；空输入推断并行度下限为 1，确保能发送结束信号 | 真实空目录扫描及三个进程分别打开 index 0/1/2、parallel=3；既有 file SQL 扫描回归 |
| 链式算子窗口上下文停留在首次窗口 | 算子 open 时克隆 RuntimeContext，worker 的后续窗口更新没有传入克隆对象。sink 一直看见窗口 1，按 window/task 幂等输出会覆盖先前结果。`AbstractProcessor.init` 现在更新当前算子及全部链式子算子的窗口上下文 | `WindowRuntimeContextTest` 验证 1→3→5→3（回放）；完整 SIZE 结果与故障重放验证 |
| DSL runtime JDK 17 测试参数丢失 | 模块自定义 Surefire argLine 覆盖父级开放模块参数；补回 `${extraJavaTestArgs}`，并限制测试 JVM 可见 CPU 数 | JDK 17 下既有 `TableScanTest` 6 项（含空输入）通过 |

窗口修复影响公共流处理链路，不能只用 Iceberg 的单元测试证明正确。因此额外运行 processor、runtime context 和既有 file SQL 测试。图计算自身的迭代初始化逻辑保持原有路径。

### 7.3 可审计的验收契约

数据集含 12 个 Parquet 文件、480 个顺序 ID、position delete（7）、equality delete（23），另追加 ID 40/41 两条重复业务行。SQL 过滤 `id >= 5` 后应精确输出 **475 行**。比对的是 `(id,payload)→出现次数` 的完整多重集，不能用集合去重掩盖丢失或重放重复。

故障期间再追加 10000–10004 的新 snapshot；恢复后的 475 行必须排除它们。计划根 JSON 在故障前后完全相同，同一 `(window,task)` 必须出现不同 PID 的执行记录，并检查 master 非零退出、恢复标记及 driver 的持久 pipeline 恢复日志。测试 sink 每个 window/task 使用原子事务替换，因此能观察重放并校验最终输出；这是显式的幂等测试 sink 契约，不意味着任意生产 sink 获得 exactly-once。

每个场景将组件日志、初始/恢复 PID、故障前后计划、事务尝试、窗口结果行数和 offset 数保存到唯一的 `target/cluster-*` 目录。`acceptance.txt` 只在全部断言通过后生成。测试关闭自建 JVM、删除自己的随机 PG schema；临时服务不接触用户业务数据库。

CI 已增加 Redis service、完整运行时构建、公共回归和多进程验收步骤；任何必要服务缺失都会失败，不使用 skip 伪装通过。诊断 artifact 包含日志、PID、计划和数据库审计摘要，排除配置凭证文件及大体积 warehouse/checkpoint。远端 GitHub Actions 尚未触发。


### 7.4 最终验证结果

本轮完成 **37 个不同测试用例，0 失败、0 跳过**；另对 flying-kill 使用密码认证 PostgreSQL 复验一次通过，不重复计入 37。这里的总数按测试方法/数据集用例去重，isolated IT 在混合检查中的重复运行也不重复计数。

| 测试组 | 用例数 | 本轮结果 |
| --- | ---: | --- |
| Iceberg 单元/功能测试 | 17 | 全部通过 |
| isolated / PostgreSQL plan / mixed-runtime IT | 3 | 全部通过；mixed 内含两种 classpath 顺序 |
| OffsetStore / JdbcPoolIsolation | 2 | 全部通过 |
| 多进程 ALL / SIZE / size-kill / flying-kill / graph | 5 | 全部通过，整组约 268 秒 |
| WindowRuntimeContextTest | 1 | 通过，推进和回放窗口均传给链式 sink |
| 既有 processor 测试 | 3 | 全部通过 |
| TableScanTest | 6 | 全部通过，含新增真实空目录扫描 |
| **合计** | **37** | **全部通过** |

多进程验收在 Temurin 17.0.21 / Maven 3.9.11 / PostgreSQL 17.12 / Redis 7.4.12、本机独立 JVM 上完成。每个 worker 的堆上限 512 MiB；这些时间只作为执行证据，不作为吞吐或恢复 SLA。

| 场景 | 结果行数 | 恢复与证据目录（相对验收模块 target） |
| --- | ---: | --- |
| ALL + filter + sort/shuffle | 475 | `cluster-all-1913981363228863004` |
| SIZE=10 | 475 | `cluster-size-12135404505913910486` |
| SIZE=10 / flying=1 / SIGKILL | 475 | `cluster-size-kill-10823500830471278555` |
| SIZE=10 / flying=3 / SIGKILL | 475 | `cluster-flying-kill-18235413418074975460` |
| Iceberg → graph vertices → MATCH | 473 | `cluster-graph-6989603685289260060`；重复业务 ID 按图顶点身份合并 |
| 密码认证 PostgreSQL / flying=3 复验 | 475 | `cluster-flying-kill-17868052613038623325` |

例如 flying=1 故障前，窗口 1/2 已各输出 90 行、计划 offset 表存在 47 条记录；driver 已完成 checkpoint 1/2。窗口 3 task 0 在 PID 48557 中进入未提交事务后被杀，恢复后由 PID 48943 重放，最终保留窗口 1/2 且总输出严格为 475 行。这些值来自 `before-kill.txt`、`database-evidence.txt` 与 driver 日志，不是根据配置推测。

五场景完整 Failsafe 报告保存在本地验收模块 `target/acceptance-suite-reports`；随后的认证复验报告在 `target/failsafe-reports`。对应构建日志为 `/private/tmp/geaflow-iceberg-tools/{cluster-suite,cluster-auth,p1-final-module,p1-final-mixed,p1-foundation-test,processor-tests,window-context-test,table-scan-final}.log`。target 与临时目录属于本次工作区证据，不是版本库中的永久制品；CI 每次运行将按工作流上传报告。

本轮 Checkstyle、Apache RAT、差异空白检查和工作流 YAML 解析通过。检查确认没有遗留测试集群 JVM；本次创建的 PostgreSQL/Redis 临时容器已停止移除。

### 7.5 发布边界与剩余准入项

**需要统一构建和发布 connector、operator、processor、DSL runtime 的修改。只替换 Iceberg JAR 不会修复窗口上下文或并行度问题。** 原默认 JDK 8/11 发布路径与 profile 隔离保持不变，但本轮公共运行时修改的执行回归使用 JDK 17，不能替代 JDK 8/11 的发布回归。最终发行包仍需按第 5 节从最终源码构建验收。

当前已补齐同机真实多 worker 调度、worker 进程死亡、持久 driver/coordinator 恢复、flying=1/3 和基础 SQL/graph 端到端。仍保留以下生产准入事项：

- 目标 Ray/Kubernetes 的跨机器重启/网络分区、独立 driver/master 故障注入、反复故障恢复和扩缩容。
- 生产 HDFS/S3/REST 认证、对象存储一致性、凭据续期及远端 checkpoint。
- PostgreSQL commit 成功但 ACK 丢失、数据库主从切换与连接耗尽。
- 百 GB/TB、长时稳定性、复杂图查询与图阶段故障恢复；当前 graph 场景只验证入点后 MATCH 的正常链路。
- 默认发行版本 JDK 8/11 回归、包含前端资源的完整发行包及目标部署平台验收。

因此本轮可认定 **P1 的同机多进程功能与恢复验收通过**，不能标记全部生产环境准入完成。本节验证结果记录于 PR 提交前。
