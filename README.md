# IoT 设备模拟器 — 多设备发送配置手册

## 一、整体架构

```
┌──────────────────────────────────────────────────────────────┐
│  iot_device_simulator.py (本地运行)                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │ wsd-01   │ │ cn-01    │ │ device-6 │ │ device-28│ ...   │
│  │ 温湿度计  │ │ 储能系统  │ │ 电力计量  │ │ 锅炉     │       │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘       │
│       │            │            │            │              │
│       └────────────┴────────────┴────────────┘              │
│                         │ MQTT/AMQP                         │
└─────────────────────────┼───────────────────────────────────┘
                          ▼
                 ┌─────────────────┐
                 │  Azure IoT Hub  │  ← 自动注册设备 (Service SDK)
                 └────────┬────────┘
                          │ (消息路由)
                          ▼
                 ┌─────────────────┐
                 │  Azure Event Hub │ (内置端点或自定义端点)
                 └────────┬────────┘
                          │ (Capture → Avro)
                          ▼
                 ┌─────────────────┐
                 │  Azure ADLS Gen2 │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  Databricks      │
                 └─────────────────┘
```

## 二、环境准备

### 2.1 Python 依赖安装

```powershell
pip install azure-iot-device azure-iot-hub
```

| 包名 | 用途 | 最低版本 |
|---|---|---|
| `azure-iot-device` | 设备端 SDK，模拟设备发送遥测 | 2.12+ |
| `azure-iot-hub` | 服务端 SDK，自动注册/管理设备 | 2.6+ |

### 2.2 Azure 资源准备

在 Azure Portal 上确保以下资源就绪：

| 资源 | 说明 |
|---|---|
| **IoT Hub** | 任意层级 (F1 免费版即可测试, 每天 8000 条消息) |
| **Event Hub** (可选) | IoT Hub 内置兼容端点已包含 Event Hub；如需自定义路由则另建 |
| **Storage Account + ADLS Gen2** (可选) | 配合 Event Hub Capture 使用 |

### 2.3 获取 IoT Hub 连接字符串

这是多设备模式的**唯一必要凭据**，脚本会自动用它注册所有设备。

```
Azure Portal → IoT Hub → 共享访问策略 → iothubowner → 主连接字符串
```

格式：
```
HostName=<your-hub>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<base64-key>
```

> **安全提示**: `iothubowner` 拥有全部权限。生产环境应使用 `registryReadWrite` + `serviceConnect` 策略，或使用 DPS。

---

## 三、快速开始 (3 步)

### 步骤 1：生成配置文件

```powershell
cd C:\Users\JieYin\Downloads\iot_simulator
python iot_device_simulator.py --init-config
```

生成 `simulator_config.json`：

```json
{
  "iothub_connection_string": "HostName=<your-hub>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<key>",
  "send_interval_seconds": 10,
  "message_count": 0,
  "devices": [
    {"device_id": "device-wsd-01", "device_type": "temperature_humidity"},
    {"device_id": "device-wsd-02", "device_type": "temperature_humidity"},
    {"device_id": "device-cn-01",  "device_type": "energy_storage"},
    {"device_id": "my-device-6",   "device_type": "power_meter"},
    {"device_id": "my-device-3",   "device_type": "solar_panel"},
    {"device_id": "my-device-28",  "device_type": "boiler"},
    {"device_id": "my-device-16",  "device_type": "chiller"},
    {"device_id": "my-device-55",  "device_type": "compressed_air"},
    {"device_id": "my-device-77",  "device_type": "substation_meter"},
    {"device_id": "my-device-19",  "device_type": "gas_flow"},
    {"device_id": "my-device-65",  "device_type": "water_meter"}
  ]
}
```

### 步骤 2：填入 IoT Hub 连接字符串

编辑 `simulator_config.json`，将 `iothub_connection_string` 替换为真实值。

### 步骤 3：启动多设备模拟

```powershell
python iot_device_simulator.py --mode multi --config simulator_config.json
```

脚本会自动完成：
1. 在 IoT Hub 上注册所有设备（已存在则跳过）
2. 获取每个设备的 SAS 密钥
3. 为每个设备建立独立 MQTT 连接
4. 按配置的间隔并发发送遥测数据

---

## 四、配置文件详解

### 4.1 全局参数

| 字段 | 类型 | 说明 |
|---|---|---|
| `iothub_connection_string` | string | IoT Hub 服务连接字符串 (含 `SharedAccessKeyName`) |
| `send_interval_seconds` | number | 每个设备的发送间隔 (秒) |
| `message_count` | number | 每个设备发送的消息总数 (0 = 无限循环) |

### 4.2 设备列表

每个设备条目包含：

| 字段 | 说明 |
|---|---|
| `device_id` | 设备ID，将在 IoT Hub 上注册此 ID |
| `device_type` | 设备类型，决定生成的遥测数据格式 |

### 4.3 可用设备类型

| device_type | 说明 | 测点数 | 数据特征 |
|---|---|---|---|
| `temperature_humidity` | 温湿度计 | 7 | 温度 15~40°C, 湿度 10~80% |
| `energy_storage` | 储能 PCS/BMS | 100 | 电池SOC, 充放电状态, IGBT温度 |
| `power_meter` | 配电回路电表 | 42 | 10个回路的电能(递增)+三相电流 |
| `solar_panel` | 光伏并网柜 | 14 | 3组光伏的有功电能+三相电流 |
| `boiler` | 锅炉系统 | 32 | 2台锅炉温度/压力/运行状态 |
| `chiller` | 冷水机组 | 43 | 3台主机+水泵+冷却塔运行数据 |
| `compressed_air` | 压缩空气流量计 | 10 | 瞬时/累计流量, 温度, 压力 |
| `substation_meter` | 变电所多回路电表 | 42 | 5个回路的电压/电流/电能/负载 |
| `gas_flow` | 天然气流量计 | 6 | 4条产线天然气用量 |
| `water_meter` | 水表+燃气表 | 11 | 多路累计流量+燃气瞬时/累计 |

### 4.4 自定义设备示例

只需在 `devices` 数组中添加条目即可增加设备：

```json
{
  "devices": [
    {"device_id": "factory-A-wsd-01", "device_type": "temperature_humidity"},
    {"device_id": "factory-A-wsd-02", "device_type": "temperature_humidity"},
    {"device_id": "factory-A-boiler",  "device_type": "boiler"},
    {"device_id": "factory-B-power",   "device_type": "power_meter"}
  ]
}
```

---

## 五、运行模式参考

### 5.1 Dry-Run 验证 (不连接 IoT Hub)

```powershell
# 多设备 dry-run, 每设备发 2 条
python iot_device_simulator.py --mode multi --dry-run --count 2 --interval 5

# 使用配置文件 dry-run
python iot_device_simulator.py --mode multi --config simulator_config.json --dry-run --count 1
```

### 5.2 单设备调试

```powershell
# 环境变量方式
$env:IOTHUB_DEVICE_CONNECTION_STRING = "HostName=xxx;DeviceId=device-wsd-01;SharedAccessKey=xxx"
python iot_device_simulator.py --device-id device-wsd-01 --interval 5

# 命令行参数方式
python iot_device_simulator.py --device-id my-device-28 --interval 5 `
  --connection-string "HostName=xxx;DeviceId=my-device-28;SharedAccessKey=xxx"

# 自定义设备ID + 指定类型
python iot_device_simulator.py --device-id test-boiler-01 --device-type boiler --dry-run
```

### 5.3 多设备正式运行

```powershell
# 方式 A: 配置文件 (推荐)
python iot_device_simulator.py --mode multi --config simulator_config.json

# 方式 B: 环境变量
$env:IOTHUB_CONNECTION_STRING = "HostName=xxx;SharedAccessKeyName=iothubowner;SharedAccessKey=xxx"
python iot_device_simulator.py --mode multi --interval 15 --count 100
```

### 5.4 限量发送 (测试用)

```powershell
# 每个设备发 50 条消息后自动停止
python iot_device_simulator.py --mode multi --config simulator_config.json --count 50
```

### 5.5 停止运行

按 `Ctrl+C`，所有设备线程将优雅退出。

---

## 六、验证数据到达

### 6.1 Azure Portal 验证

```
IoT Hub → 概述 → 使用情况图表
```

查看 "发送的设备到云消息" 计数是否增长。

### 6.2 Azure CLI 监控

```powershell
# 安装 IoT 扩展
az extension add --name azure-iot

# 实时监控所有设备消息
az iot hub monitor-events --hub-name <your-hub> --output table

# 只监控特定设备
az iot hub monitor-events --hub-name <your-hub> --device-id device-wsd-01
```

### 6.3 Event Hub Capture 验证

如已配置 Event Hub Capture → ADLS：

```
Storage Account → 容器 → <capture-container>
  → <namespace>/<eventhub>/<partition>/<year>/<month>/<day>/<hour>/<minute>/
    → *.avro 文件
```

### 6.4 Databricks 端验证

```python
df = (spark.read.format("avro")
      .load("abfss://<container>@<storage>.dfs.core.windows.net/<path>/**/*.avro"))

df.select(
    "SequenceNumber",
    "EnqueuedTimeUtc",
    F.col("SystemProperties").getItem("iothub-connection-device-id").alias("DeviceId"),
    F.col("Body").cast("string").alias("Body")
).show(truncate=80)
```

---

## 七、常见问题

### Q1: 免费版 IoT Hub (F1) 有什么限制？

| 限制项 | F1 免费层 | S1 标准层 |
|---|---|---|
| 每日消息数 | 8,000 | 400,000 |
| 设备数 | 500 | 无限制 |
| 消息大小 | 0.5 KB | 256 KB |

建议测试时：设置 `--count 50 --interval 30` 控制消息量。

### Q2: 报错 `ImportError: No module named 'azure.iot.hub'`

```powershell
pip install azure-iot-hub
```

这个包只在多设备模式下需要（用于自动注册设备）。单设备模式只需 `azure-iot-device`。

### Q3: 报错 `Unauthorized`

检查连接字符串：
- **多设备模式** 需要 **服务连接字符串** (含 `SharedAccessKeyName`)
- **单设备模式** 需要 **设备连接字符串** (含 `DeviceId`)

### Q4: 如何调整发送频率？

- 配置文件: 修改 `send_interval_seconds`
- 命令行: `--interval 30` (30秒一次)
- 建议: 测试用 5~10 秒, 模拟生产用 30~60 秒

### Q5: 设备已存在但密钥不同怎么办？

脚本会读取已有设备的密钥，不会覆盖。如需重置：
```powershell
az iot hub device-identity delete --hub-name <hub> --device-id <device-id>
```

### Q6: 如何只运行部分设备？

编辑 `simulator_config.json`，删除不需要的设备条目即可。

---

## 八、完整命令参考

```
python iot_device_simulator.py [选项]

选项:
  --mode {single,multi}      运行模式 (默认: single)
  --device-id ID             单设备模式的设备ID (默认: device-wsd-01)
  --device-type TYPE         设备类型
  --interval SECONDS         发送间隔 (默认: 10)
  --count N                  消息数量, 0=无限 (默认: 0)
  --dry-run                  仅生成不发送
  --connection-string STR    连接字符串
  --config PATH              多设备配置文件路径
  --init-config              生成默认配置文件模板
  --list-devices             列出预定义设备
```

### 环境变量

| 变量名 | 用途 |
|---|---|
| `IOTHUB_DEVICE_CONNECTION_STRING` | 单设备模式 - 设备连接字符串 |
| `IOTHUB_CONNECTION_STRING` | 多设备模式 - 服务连接字符串 |

---

## 九、部署到 Azure (容器化运行)

本地调试完成后，可以将模拟器部署到 Azure 持续运行，无需保持本地电脑开机。

### 方案对比

| 方案 | 适用场景 | 成本 | 复杂度 |
|---|---|---|---|
| **Azure Container Instance (ACI)** | 持续运行，简单快速 | ~￥1.5/天 (0.5vCPU) | ★☆☆ |
| Azure Container Apps Job | 定时触发批量发送 | 按次计费 | ★★☆ |
| Azure VM | 需要完整 OS 控制 | ~￥50/月起 | ★★☆ |

**推荐 ACI** — 一条命令部署，按秒计费，随时停止。

### 9.1 前置条件

```powershell
# 确认 Azure CLI 已登录
az login
az account show --query name -o tsv

# 确认当前目录
cd C:\Users\JieYin\Downloads\iot_simulator
```

### 9.2 一键部署 (推荐)

```powershell
.\deploy-to-azure.ps1 `
    -ResourceGroup "rg-iot-simulator" `
    -Location "eastasia" `
    -IoTHubConnectionString "HostName=<hub>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<key>" `
    -SendInterval 10 `
    -MessageCount 0
```

脚本自动完成：
1. 创建资源组
2. 创建 Azure Container Registry (ACR)
3. 在云端构建 Docker 镜像 (无需本地安装 Docker)
4. 部署为 Azure Container Instance
5. 13 个设备开始并发发送遥测数据

### 9.3 手动分步部署

如果需要更细粒度控制，可以手动执行每个步骤：

#### Step 1: 创建资源

```powershell
$RG = "rg-iot-simulator"
$LOCATION = "eastasia"
$ACR = "acriotsim01"

az group create --name $RG --location $LOCATION
az acr create --resource-group $RG --name $ACR --sku Basic --admin-enabled true
```

#### Step 2: 云端构建镜像

```powershell
# 在 ACR 中直接构建 (无需本地 Docker)
az acr build --registry $ACR --image iot-device-simulator:latest --file Dockerfile .
```

#### Step 3: 部署容器

```powershell
$ACR_SERVER = az acr show --name $ACR --query loginServer -o tsv
$ACR_USER   = az acr credential show --name $ACR --query username -o tsv
$ACR_PASS   = az acr credential show --name $ACR --query "passwords[0].value" -o tsv

$IOT_CONN = "HostName=<hub>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<key>"

az container create `
    --resource-group $RG `
    --name iot-simulator `
    --image "$ACR_SERVER/iot-device-simulator:latest" `
    --registry-login-server $ACR_SERVER `
    --registry-username $ACR_USER `
    --registry-password $ACR_PASS `
    --cpu 0.5 --memory 0.5 `
    --restart-policy Always `
    --command-line "python iot_device_simulator.py --mode multi --interval 10 --count 0"  `
    --environment-variables "IOTHUB_CONNECTION_STRING=$IOT_CONN"
```

### 9.4 运维操作

```powershell
# 查看实时日志
az container logs --resource-group rg-iot-simulator --name iot-simulator --follow

# 停止 (暂停计费)
az container stop --resource-group rg-iot-simulator --name iot-simulator

# 重新启动
az container start --resource-group rg-iot-simulator --name iot-simulator

# 查看状态
az container show --resource-group rg-iot-simulator --name iot-simulator `
    --query "{Status:instanceView.state, CPU:containers[0].resources.requests.cpu, Memory:containers[0].resources.requests.memoryInGB}" -o table

# 删除所有资源 (一键清理)
az group delete --name rg-iot-simulator --yes --no-wait
```

### 9.5 更新镜像后重新部署

修改代码或配置后：

```powershell
# 重新构建
az acr build --registry $ACR --image iot-device-simulator:latest --file Dockerfile .

# 重新创建容器 (ACI 不支持原地更新镜像)
az container delete --resource-group $RG --name iot-simulator --yes
az container create ... # 同 Step 3
```

### 9.6 使用自定义配置文件部署

如果需要自定义设备列表，先编辑 `simulator_config.json`，然后重新构建镜像：

```powershell
# 编辑配置
notepad simulator_config.json

# 重新构建 (配置文件会被打包进镜像)
az acr build --registry $ACR --image iot-device-simulator:latest --file Dockerfile .

# 部署时使用配置文件
az container create `
    --resource-group $RG `
    --name iot-simulator `
    --image "$ACR_SERVER/iot-device-simulator:latest" `
    --registry-login-server $ACR_SERVER `
    --registry-username $ACR_USER `
    --registry-password $ACR_PASS `
    --cpu 0.5 --memory 0.5 `
    --restart-policy Always `
    --command-line "python iot_device_simulator.py --mode multi --config simulator_config.json" `
    --secure-environment-variables "IOTHUB_CONNECTION_STRING=$IOT_CONN"
```

> **提示**: 使用 `--secure-environment-variables` 代替 `--environment-variables` 可以在 Portal 中隐藏连接字符串。

### 9.7 费用估算

| 配置 | 单价 (East Asia) | 月费用 (持续运行) |
|---|---|---|
| 0.5 vCPU + 0.5 GB | ~$0.05/天 | ~$1.5/月 |
| 1 vCPU + 1 GB | ~$0.10/天 | ~$3/月 |
| ACR Basic | $0.167/天 | ~$5/月 |

> 停止容器后 ACI 不计费。测试完成后可执行 `az group delete` 一键清理。
