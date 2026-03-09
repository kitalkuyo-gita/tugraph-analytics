# PaddlePaddle 推理框架支持 - 生产部署指南

## 📋 概述

本指南详细说明如何在 GeaFlow 中部署和使用 PaddlePaddle (飞桨) 推理框架，支持 PaddleSpatial 图神经网络模型。

## 🏗️ 架构设计

### 核心设计理念

GeaFlow 采用**策略模式**实现推理框架的可插拔扩展：

```
Java 主进程 → InferContext → infer_server.py → [TorchInferSession | PaddleInferSession]
                                              ↓
                                    共享内存 (mmap_ipc) ↔ Pickle 序列化
```

### 关键组件

| 组件 | 文件 | 作用 |
|------|------|------|
| **抽象基类** | `baseInferSession.py` | 定义框架无关的 `run()` 接口 |
| **PyTorch 会话** | `inferSession.py` | PyTorch 推理实现 |
| **PaddlePaddle 会话** | `paddleInferSession.py` | PaddlePaddle 推理实现（动态/静态图） |
| **分派器** | `infer_server.py` | 根据 `--framework` 参数选择 Session |
| **安装脚本** | `install-infer-env.sh` | 自动安装 PaddlePaddle 依赖 |

## 🚀 快速开始

### 1. Java 侧配置

```java
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;

// ... 在构建 GraphJob 时配置
envConfig.put(FrameworkConfigKeys.INFER_ENV_ENABLE, "true");
envConfig.put(FrameworkConfigKeys.INFER_FRAMEWORK_TYPE, "PADDLE");  // 关键！
envConfig.put(FrameworkConfigKeys.INFER_ENV_PADDLE_GPU_ENABLE, "true");  // GPU 加速
envConfig.put(FrameworkConfigKeys.INFER_ENV_PADDLE_CUDA_VERSION, "12.0");  // CUDA 版本
envConfig.put(FrameworkConfigKeys.INFER_ENV_INIT_TIMEOUT_SEC, "1800");  // 30 分钟超时
envConfig.put(FrameworkConfigKeys.INFER_ENV_USER_TRANSFORM_CLASSNAME, 
              "PaddleGCNTransform");  // UDF 类名
```

### 2. Python UDF 实现

创建 `TransFormFunctionUDF.py` 文件：

```python
from TransFormFunction import TransFormFunction
import paddle
import pgl
import numpy as np


class PaddleGCNTransform(TransFormFunction):
    def __init__(self):
        super().__init__(1)  # 接收 1 个输入参数
        self.device = 'gpu' if paddle.device.is_compiled_with_cuda() else 'cpu'
        self.load_model('model.pdparams')
        
    def load_model(self, model_path: str):
        """加载飞桨模型"""
        self.model = GCN(...)  # 定义模型结构
        state_dict = paddle.load(model_path)
        self.model.set_state_dict(state_dict)
        self.model.eval()
    
    def transform_pre(self, *args):
        """预处理：构建 PGL 图对象"""
        vertex_id = args[0]
        # 构建子图、提取特征...
        graph = pgl.Graph(num_nodes=..., edges=..., node_feat={...})
        features = graph.node_feat['feat']
        return (graph, features), features  # 必须是 numpy 或可 pickle 对象
    
    def transform_post(self, result):
        """后处理：转换结果为 Python 列表"""
        if isinstance(result, paddle.Tensor):
            result = result.numpy()
        probs = self._softmax(result[0])
        predicted_class = int(np.argmax(probs))
        return [float(probs[predicted_class]), predicted_class]
```

### 3. 打包与部署

#### 步骤 1: 准备模型文件

```bash
# 训练好的 PaddlePaddle 模型
model.pdparams      # 参数文件
model.pdmodel       # 模型结构（可选，静态图需要）
model.pdiparams     # 推理参数（可选，静态图需要）
```

#### 步骤 2: 打包 UDF JAR

```bash
# 目录结构
udf-jar/
├── TransFormFunctionUDF.py          # 你的 UDF 实现
├── TransFormFunction.py             # 基类（从 GeaFlow 获取）
└── model.pdparams                   # 模型文件

# 打包为 ZIP/JAR
jar cvf udf.jar TransFormFunctionUDF.py model.pdparams
```

#### 步骤 3: 上传到 OSS

```bash
# 使用 ossutil 上传
ossutil cp udf.jar oss://your-bucket/path/to/udf.jar
ossutil cp model.pdparams oss://your-bucket/path/to/model.pdparams
```

### 4. 提交作业

```java
// 设置 UDF jar 包路径
envConfig.put(FrameworkConfigKeys.INFER_USER_DEFINE_LIB_PATH, 
              "oss://your-bucket/path/to/udf.jar");

// 提交作业
GraphJob job = new GraphJob(envConfig);
job.run();
```

## 🔧 高级配置

### GPU vs CPU 推理

| 场景 | 配置 | 说明 |
|------|------|------|
| **GPU 推理** | `INFER_ENV_PADDLE_GPU_ENABLE=true` | 生产推荐，需指定 CUDA 版本 |
| **CPU 推理** | `INFER_ENV_PADDLE_GPU_ENABLE=false` | 开发测试，无需 GPU |

### CUDA 版本匹配

```properties
# CUDA 11.7
geaflow.infer.env.paddle.cuda.version=11.7
→ 自动安装 paddlepaddle-gpu==2.5.0

# CUDA 12.0
geaflow.infer.env.paddle.cuda.version=12.0
→ 自动安装 paddlepaddle-gpu==2.6.0.post120
```

### 静态图 vs 动态图推理

```python
# 动态图模式（开发调试）
class MyTransform(TransFormFunction):
    infer_mode = "dynamic"  # 默认值
    # 直接调用 model(input)

# 静态图模式（生产部署，性能更优）
class MyTransform(TransFormFunction):
    infer_mode = "static"
    model_dir = "/path/to/model"  # .pdmodel + .pdiparams
    # 使用 paddle.inference.create_predictor()
```

## 📊 性能优化建议

### 1. 共享内存队列大小调整

PaddleSpatial GNN 模型通常有高维特征，建议调整：

```java
// 默认 8MB，对于高维特征可能不足
envConfig.put(FrameworkConfigKeys.INFER_ENV_SHARE_MEMORY_QUEUE_SIZE, 
              String.valueOf(32 * 1024 * 1024));  // 32MB
```

### 2. 批量推理

如果单次推理数据量小，考虑 batch 累积：

```python
def transform_pre(self, *args):
    # 累积多个顶点再推理
    batch_vertices = self.buffer.get_batch(32)
    return batch_features  # Shape: [batch_size, feature_dim]
```

### 3. PGL 图缓存

避免重复构建图对象：

```python
class PaddleGCNTransform(TransFormFunction):
    def __init__(self):
        self.graph_cache = {}  # LRU cache
        
    def transform_pre(self, vertex_id):
        if vertex_id not in self.graph_cache:
            graph = self.build_subgraph(vertex_id)
            self.graph_cache[vertex_id] = graph
        return self.graph_cache[vertex_id]
```

## 🐛 故障排查

### 问题 1: Python 子进程启动失败

**症状**: `get pipeline result error`

**检查**:
```bash
# 查看 infer_server.py 日志
tail -f /path/to/infer/logs/*.log

# 常见原因：
# - PaddlePaddle 未正确安装
# - CUDA 版本不匹配
# - 模型文件路径错误
```

**解决**:
```properties
# 增加初始化超时
geaflow.infer.env.init.timeout.sec=1800

# 启用详细日志
logging.basicConfig(level=logging.DEBUG)
```

### 问题 2: Pickle 序列化失败

**症状**: `pickle.PicklingError: Can't pickle <class 'paddle.Tensor'>`

**原因**: Paddle Tensor 不能直接 pickle

**解决**:
```python
def transform_pre(self, *args):
    # ❌ 错误：返回 Paddle Tensor
    # return paddle_tensor
    
    # ✅ 正确：转换为 numpy
    return paddle_tensor.numpy()
```

### 问题 3: GPU 显存不足

**症状**: `CUDA out of memory`

**解决**:
1. 减小 batch size
2. 降低特征维度
3. 使用混合精度推理（FP16）

```python
# Paddle Inference 配置
config.enable_use_gpu(200, 0)  # 限制显存使用
config.enable_mkldnn()  # CPU 加速
```

## 📝 完整示例

参考以下完整示例文件：
- `PaddleSpatialUDFExample.py` - 完整的 UDF 实现
- `requirements_paddle.txt` - PaddlePaddle 依赖配置
- `paddleInferSession.py` - PaddleInferSession 实现

## 🎯 最佳实践清单

- [ ] 确认 CUDA 版本与 PaddlePaddle wheel 匹配
- [ ] 将 `paddle.Tensor` 转为 `numpy` 再返回
- [ ] 生产环境使用静态图推理（`infer_mode="static"`）
- [ ] 设置合理的超时时间（1800 秒）
- [ ] 监控 Python 子进程日志
- [ ] 使用 GPU 时预留足够显存
- [ ] 模型文件与代码一起打包上传

## 📚 参考资料

- [PaddlePaddle 官方文档](https://www.paddlepaddle.org.cn/documentation/docs/zh/guide/index_cn.html)
- [PGL 图学习库](https://github.com/PaddlePaddle/PGL)
- [GeaFlow UDF 快速入门](docs/docs-cn/source/3.quick_start/3.quick_start_infer&UDF.md)

---

**版本**: v1.0  
**最后更新**: 2026-03-07  
**维护者**: Apache GeaFlow Team
