"""
IoT Device Simulator - 基于真实 Avro 数据样本生成模拟遥测数据并发送到 Azure IoT Hub

数据链路: IoT Hub → Event Hub → Azure ADLS → Databricks

用法:
  1. 安装依赖:  pip install azure-iot-device
  2. 设置连接字符串环境变量 (见下方 CONFIG 部分)
  3. 运行:  python iot_device_simulator.py

支持模式:
  --mode single    单设备模式 (默认)
  --mode multi     多设备并发模式
  --device-id      指定设备 ID
  --interval       发送间隔 (秒, 默认 10)
  --count          发送消息数量 (默认 0 = 无限)
  --dry-run        仅打印消息不发送
"""

import argparse
import json
import random
import time
import os
import sys
import signal
import threading
from datetime import datetime, timezone
from pathlib import Path

# ============================================================================
# CONFIG - IoT Hub 连接方式 (三选一, 按优先级)
# ============================================================================
# 方式1: 设备连接字符串 (单设备模式)
#   设置环境变量: IOTHUB_DEVICE_CONNECTION_STRING
#   格式: HostName=<hub>.azure-devices.net;DeviceId=<id>;SharedAccessKey=<key>
#
# 方式2: DPS 对称密钥 (多设备模式, 自动注册)
#   设置环境变量: DPS_ID_SCOPE, DPS_SYMMETRIC_KEY, DPS_REGISTRATION_ID
#
# 方式3: IoT Hub 连接字符串 + 设备 ID (用 IoT Hub SDK 创建设备)
#   设置环境变量: IOTHUB_CONNECTION_STRING
# ============================================================================


# ============================================================================
# 设备模板定义 - 基于真实 Avro 数据样本提取
# ============================================================================

def _jitter(base_value, pct=0.05):
    """在基准值上添加 ±pct 的随机抖动"""
    if base_value == 0:
        return 0.0
    delta = abs(base_value) * pct
    return round(base_value + random.uniform(-delta, delta), 2)


def _jitter_int(base_value, pct=0.05):
    """整数抖动"""
    if base_value == 0:
        return 0
    delta = max(1, int(abs(base_value) * pct))
    return base_value + random.randint(-delta, delta)


def _binary_signal(probability=0.5):
    """生成 0/1 信号"""
    return 1.0 if random.random() < probability else 0.0


class DeviceTemplate:
    """设备数据模板基类"""

    def __init__(self, device_id, device_type):
        self.device_id = device_id
        self.device_type = device_type
        # 累积量基准值 (模拟电能表等递增值)
        self._accumulators = {}

    def _accumulate(self, key, base_value, increment_range=(0.1, 2.0)):
        """模拟累积量递增"""
        if key not in self._accumulators:
            self._accumulators[key] = base_value
        self._accumulators[key] += random.uniform(*increment_range)
        return round(self._accumulators[key], 2)

    def generate(self):
        raise NotImplementedError


class TemperatureHumidityDevice(DeviceTemplate):
    """温湿度计设备 (device-wsd-xx)"""

    def __init__(self, device_id):
        super().__init__(device_id, "temperature_humidity")

    def generate(self):
        return {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "#DISABLE_DEVICE_温湿度计": 0,
            "#BATCH_WRITE_温湿度计": 0,
            "#DEVICE_ERROR_温湿度计": 0.0,
            "温湿度计:wd": round(random.uniform(15.0, 40.0), 2),  # 温度 15~40°C
            "温湿度计:sd": round(random.uniform(10.0, 80.0), 2),  # 湿度 10~80%
        }


class PowerMeterDevice(DeviceTemplate):
    """电力计量设备 (my-device-6 类型 - 配电回路电能/电流)"""

    CIRCUITS = [
        ("2_变L201", 12777400, 970),
        ("L205_2西门卫", 1475, 1.5),
        ("L205_3备用", 6618, 14.5),
        ("L204_31_2_空压机", 24461, 0),
        ("L206_2配电所用电", 12418, 0.15),
        ("L206_4东门卫", 4594, 9),
        ("L207_1L13线烤炉3_空压机2_干燥机", 15552, 100),
        ("L207_3L13打馅间", 4451, 30),
        ("L208_1L13线包装", 1485, 22),
        ("L209_2L13包装区", 4618, 18),
    ]

    def __init__(self, device_id):
        super().__init__(device_id, "power_meter")

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        }
        for circuit_name, energy_base, current_base in self.CIRCUITS:
            data[f"{circuit_name}组合有功电能"] = self._accumulate(
                f"{circuit_name}_energy", energy_base, (0.5, 5.0)
            )
            data[f"{circuit_name}A相电流"] = _jitter(current_base, 0.1)
            data[f"{circuit_name}B相电流"] = _jitter(current_base, 0.1)
            data[f"{circuit_name}C相电流"] = _jitter(current_base, 0.1)
        return data


class SolarPanelDevice(DeviceTemplate):
    """光伏并网柜设备 (my-device-3)"""

    def __init__(self, device_id):
        super().__init__(device_id, "solar_panel")

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        }
        for i in range(1, 4):
            data[f"光伏{i}_并网柜:吸收有功电能"] = self._accumulate(
                f"solar_{i}_energy", 129000 + i * 500, (0.1, 3.0)
            )
            base_current = 220 + i * 5
            data[f"光伏{i}_并网柜:A相电流"] = _jitter(base_current, 0.05)
            data[f"光伏{i}_并网柜:B相电流"] = _jitter(base_current, 0.05)
            data[f"光伏{i}_并网柜:C相电流"] = _jitter(base_current, 0.05)
        return data


class BoilerDevice(DeviceTemplate):
    """锅炉设备 (my-device-28)"""

    def __init__(self, device_id):
        super().__init__(device_id, "boiler")

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        }
        for i in range(1, 3):
            running = random.choice([0, 1])
            data.update({
                f"1_2锅炉_{i}_热媒水温度": _jitter(90 + i * 5, 0.05),
                f"1_2锅炉_{i}_频率": _jitter(21.5 if running else 0, 0.1),
                f"1_2锅炉_{i}_出水温度": _jitter(88 + i * 2, 0.05),
                f"1_2锅炉_{i}_回水温度": _jitter(82 + i * 3, 0.05),
                f"1_2锅炉_{i}_炉内压力": _jitter(-5.0, 0.2),
                f"1_2锅炉_{i}_阀门开度": _jitter(17.6 if running else 0, 0.15),
                f"1_2锅炉_{i}_点火次数": self._accumulate(
                    f"boiler_{i}_ignition", 75000 + i * 1000, (0, 1)
                ),
                f"1_2锅炉_{i}_运行时间": self._accumulate(
                    f"boiler_{i}_runtime", 3000 + i * 500, (0, 0.02)
                ),
                f"1_2锅炉_{i}_运行分钟": _jitter_int(35, 0.3),
                f"1_2锅炉_{i}_运行": running,
                f"1_2锅炉_{i}_点火": 0,
                f"1_2锅炉_{i}_通讯断": random.choice([0, 1]),
            })
        # 冷热站附加参数
        data.update({
            "1_2锅炉_毕加索工艺冷水机组冷冻水温度": _jitter(-25.0, 0.1),
            "1_2锅炉_毕加索工艺冷水机组冷却水温度": _jitter(17.0, 0.1),
            "1_2锅炉_毕加索工艺板换温度": _jitter(-3.1, 0.2),
            "1_2锅炉_毕加索夹套板换温度": _jitter(9.6, 0.1),
            "1_2锅炉_配料热水温度": _jitter(63.4, 0.05),
            "1_2锅炉_夹套热水温度": _jitter(88.8, 0.05),
        })
        return data


class ChillerDevice(DeviceTemplate):
    """冷水机组设备 (my-device-16)"""

    def __init__(self, device_id):
        super().__init__(device_id, "chiller")

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        }
        for i in range(1, 4):
            running = _binary_signal(0.4)
            load_rate = round(random.uniform(40, 70), 2) if running else 0.0
            power = round(random.uniform(60, 120), 2) if running else round(random.uniform(0, 2), 1)
            data.update({
                f"{i}号主机运行信号": running,
                f"{i}号主机故障信号": 0.0,
                f"{i}号主机电功率": power,
                f"{i}号主机本期耗电量": self._accumulate(f"chiller_{i}_kwh", 30000 + i * 10000, (0, 5)),
                f"{i}号主机累计耗电量": self._accumulate(f"chiller_{i}_total_kwh", 800000 + i * 300000, (0, 5)),
                f"{i}号主机负载率": load_rate,
                f"{i}号主机冷冻水供水温度": _jitter(12.0, 0.1),
                f"{i}号主机冷冻水回水温度": _jitter(14.5, 0.1),
                f"{i}号主机冷却水出口温度": _jitter(23.0, 0.1),
                f"{i}号主机冷却水入口温度": _jitter(21.0, 0.1),
                f"{i}号主机冷冻水流量": _jitter_int(200, 0.2) if running else _jitter_int(30, 0.3),
                f"{i}号主机COP": round(random.uniform(5, 9), 2) if running else 0.0,
            })
        data.update({
            "1号冷热站水系统COP": round(random.uniform(5, 7), 2),
            "1号传感器室外温度传感器": _jitter(15.0, 0.3),
            "1号传感器室外湿度传感器": _jitter(37.0, 0.2),
            "1号传感器冷冻供水总管温度": _jitter(12.0, 0.1),
            "1号传感器冷冻回水总管温度": _jitter(14.3, 0.1),
        })
        return data


class CompressedAirDevice(DeviceTemplate):
    """压缩空气流量计设备 (my-device-55 等)"""

    METERS = [
        ("L7二楼TTM封盒机压缩空气表", 156597, 60),
        ("L7一次包装压缩空气表", 316270, 0),
    ]

    def __init__(self, device_id, meters=None):
        super().__init__(device_id, "compressed_air")
        if meters:
            self.METERS = meters

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for meter_name, cumulative_base, flow_base in self.METERS:
            data[f"{meter_name}_瞬时流量值"] = _jitter(flow_base, 0.15)
            data[f"{meter_name}_累计流量值"] = self._accumulate(
                f"{meter_name}_cum", cumulative_base, (0.1, 2.0)
            )
            data[f"{meter_name}_工况温度"] = _jitter(25.0, 0.1)
            data[f"{meter_name}_表压绝值"] = _jitter(0.6, 0.05)
        return data


class SubstationMeterDevice(DeviceTemplate):
    """变电所多回路电表设备 (my-device-77 类型 - 含电压/电流/电能/负载)"""

    CIRCUITS = [
        ("1变_D10_1_电脑房", 229.5, 155, 3847386),
        ("1变_D11_1_L2一次包装", 229.6, 87, 1375983),
        ("1变_D11_2_食堂", 229.7, 18, 1731863),
        ("1变_D12_1_L4烤炉", 229.8, 58, 2277768),
        ("1变_D15_进线总", 230.2, 600, 48326729),
    ]

    def __init__(self, device_id):
        super().__init__(device_id, "substation_meter")

    def generate(self):
        data = {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for circuit_name, voltage_base, current_base, energy_base in self.CIRCUITS:
            data[f"{circuit_name}_相电压UA"] = _jitter(voltage_base, 0.01)
            data[f"{circuit_name}_相电压UB"] = _jitter(voltage_base, 0.01)
            data[f"{circuit_name}_相电压UC"] = _jitter(voltage_base + 0.8, 0.01)
            data[f"{circuit_name}_电流A"] = _jitter(current_base, 0.1)
            data[f"{circuit_name}_电流B"] = _jitter(current_base, 0.1)
            data[f"{circuit_name}_电流C"] = _jitter(current_base, 0.1)
            data[f"{circuit_name}_有功电能总"] = self._accumulate(
                f"{circuit_name}_energy", energy_base, (0.5, 10.0)
            )
            data[f"{circuit_name}_负载"] = _jitter(current_base * 0.5, 0.15)
        return data


class EnergyStorageDevice(DeviceTemplate):
    """储能系统设备 (device-cn-01)"""

    def __init__(self, device_id):
        super().__init__(device_id, "energy_storage")
        self._soc = 50.0

    def generate(self):
        # SOC 随机缓慢变化
        self._soc = max(10, min(95, self._soc + random.uniform(-2, 2)))
        charging = random.choice([0, 1, 2])  # 0=idle, 1=charging, 2=discharging

        return {
            "aalv": 0,
            "aaotop": 0,
            "aap": _jitter_int(0, 0.5) if charging == 0 else _jitter_int(50, 0.2),
            "aapc": 0,
            "ablv": 0,
            "abpc": 0,
            "acap": 0,
            "acapv": _jitter_int(237, 0.01),
            "acbpv": _jitter_int(237, 0.01),
            "acc": charging,
            "accpv": _jitter_int(238, 0.01),
            "aclv": 0,
            "acpc": 0,
            "acrp": 0,
            "af": 50,
            "asct": _jitter_int(25, 0.05),
            "ascv": _jitter_int(3291, 0.01),
            "bmsc": 2,
            "bmuhsctn": _jitter_int(9, 0.2),
            "bmuhscvn": 0,
            "bmulsctn": 1,
            "bmulscvn": 0,
            "ccrv": 0,
            "cfstatus": 0,
            "cinc": _jitter_int(215, 0.05),
            "cinp": 100,
            "clienttime": int(time.time() * 1000),
            "cn": 0,
            "cpuremain": 0,
            "csv": 0,
            "datare": 0,
            "dc": 0,
            "dezo": 0,
            "disccrv": 0,
            "discsv": 0,
            "dp": 0.0,
            "dsp": 0,
            "dv": _jitter_int(857, 0.02),
            "emc": 1,
            "hsct": _jitter_int(26, 0.1),
            "hsctn": _jitter_int(118, 0.05),
            "hscv": _jitter_int(3292, 0.01),
            "hscvn": _jitter_int(4, 0.3),
            "igbtat": _jitter_int(50, 0.1),
            "igbtbt": _jitter_int(50, 0.1),
            "igbtct": _jitter_int(50, 0.1),
            "lceowt": _jitter(25.9, 0.05),
            "lcerwt": _jitter(25.7, 0.05),
            "lcestate": 1,
            "lcesw": 1,
            "lcet": _jitter(31.5, 0.1),
            "lcett": _jitter(23.0, 0.1),
            "lmct": 0,
            "lmonoff": 1,
            "lpvn": "V1.0",
            "lsct": _jitter_int(24, 0.1),
            "lsctn": _jitter_int(18, 0.1),
            "lscv": _jitter_int(3291, 0.01),
            "lscvn": 1,
            "maxacc": 205.0,
            "maxacp": 205000.0,
            "maxacv": 1000,
            "maxadiscc": 205.0,
            "maxadiscp": 0.0,
            "maxadiscv": 0,
            "memremain": 0,
            "mpot": 0,
            "npir": 20000,
            "occ": 0,
            "odiscc": 0,
            "otap": 0,
            "otapp": 0,
            "otrp": 0,
            "pcsc": 2,
            "pcssapr": -20,
            "pcssrpr": 0,
            "pcsstate": 2,
            "pcst": _jitter_int(28, 0.1),
            "ppir": 20000,
            "protmode": 0,
            "psap": 0,
            "psc": 0,
            "psrp": 0,
            "pss": 0,
            "psv": 0,
            "scc": _jitter_int(131, 0.05),
            "scc2": 0,
            "sccc": self._accumulate("sccc", 6713, (0, 1)),
            "scdc": self._accumulate("scdc", 5523, (0, 1)),
            "sctd": 1,
            "scvd": 1,
            "sdc": _jitter_int(129, 0.05),
            "sdiscc": 0,
            "sncode": self.device_id.replace("device-cn-", "device-cn-0"),
            "soc": round(self._soc, 1),
            "soh": 100.0,
            "tcl": 0,
            "tdcl": 0,
            "timestamp": int(time.time() * 1000),
            "workmode": 0,
        }


class GasFlowDevice(DeviceTemplate):
    """天然气流量计设备 (my-device-19)"""

    def __init__(self, device_id):
        super().__init__(device_id, "gas_flow")

    def generate(self):
        return {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "L13线_天然气用量": self._accumulate("l13_gas", 5000, (0.01, 0.5)),
            "L12线_天然气用量": self._accumulate("l12_gas", 4500, (0.01, 0.5)),
            "研发_天然气用量": self._accumulate("rd_gas", 800, (0.001, 0.1)),
            "L11线_天然气用量": self._accumulate("l11_gas", 3800, (0.01, 0.5)),
        }


class WaterMeterDevice(DeviceTemplate):
    """水表设备 (my-device-65 等)"""

    def __init__(self, device_id):
        super().__init__(device_id, "water_meter")

    def generate(self):
        return {
            "DeviceId": self.device_id,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "6线清洗间_空调冷却塔水表:累计流量": self._accumulate("w1", 12000, (0.1, 1.0)),
            "生活用水_打粉_水表:累计流量": self._accumulate("w2", 8000, (0.05, 0.5)),
            "消防水池水表:累计流量": self._accumulate("w3", 3000, (0, 0.01)),
            "锅炉QC实验室水表:累计流量": self._accumulate("w4", 5000, (0.02, 0.3)),
            "蒸汽锅炉水表:累计流量": self._accumulate("w5", 15000, (0.1, 2.0)),
            "蒸汽锅炉燃气总表_标况累计流量": self._accumulate("gas_cum", 50000, (0.1, 1.5)),
            "蒸汽锅炉燃气总表_标况瞬时流量": _jitter(120.0, 0.15),
            "蒸汽锅炉燃气总表_燃气温度": _jitter(25.0, 0.1),
            "蒸汽锅炉燃气总表_燃气绝对压力": _jitter(0.4, 0.05),
        }


# ============================================================================
# 预定义设备配置 - 模拟真实设备拓扑
# ============================================================================

DEFAULT_DEVICES = [
    # 温湿度计 x4
    ("device-wsd-01", TemperatureHumidityDevice),
    ("device-wsd-02", TemperatureHumidityDevice),
    ("device-wsd-03", TemperatureHumidityDevice),
    ("device-wsd-04", TemperatureHumidityDevice),
    # 储能系统
    ("device-cn-01", EnergyStorageDevice),
    # 电力计量
    ("my-device-6", PowerMeterDevice),
    # 光伏
    ("my-device-3", SolarPanelDevice),
    # 锅炉
    ("my-device-28", BoilerDevice),
    # 冷水机组
    ("my-device-16", ChillerDevice),
    # 压缩空气
    ("my-device-55", CompressedAirDevice),
    # 变电所电表
    ("my-device-77", SubstationMeterDevice),
    # 天然气
    ("my-device-19", GasFlowDevice),
    # 水表
    ("my-device-65", WaterMeterDevice),
]


# ============================================================================
# IoT Hub 发送逻辑
# ============================================================================

class IoTHubSender:
    """Azure IoT Hub 消息发送器"""

    def __init__(self, connection_string):
        from azure.iot.device import IoTHubDeviceClient, Message
        self._Message = Message
        self.client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        self.client.connect()
        print(f"[IoTHub] Connected successfully")

    def send(self, payload_dict):
        payload = json.dumps(payload_dict, ensure_ascii=False)
        msg = self._Message(payload)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"
        self.client.send_message(msg)

    def close(self):
        self.client.disconnect()


class DryRunSender:
    """Dry-run 模式 - 仅打印消息"""

    def send(self, payload_dict):
        pass  # 主循环会打印

    def close(self):
        pass


# ============================================================================
# 设备配置文件支持
# ============================================================================

CONFIG_TEMPLATE = """{
  "iothub_connection_string": "HostName=<your-hub>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<key>",
  "send_interval_seconds": 10,
  "message_count": 0,
  "devices": [
    {"device_id": "device-wsd-01", "device_type": "temperature_humidity"},
    {"device_id": "device-wsd-02", "device_type": "temperature_humidity"},
    {"device_id": "device-wsd-03", "device_type": "temperature_humidity"},
    {"device_id": "device-wsd-04", "device_type": "temperature_humidity"},
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
"""

TYPE_MAP = {
    "temperature_humidity": TemperatureHumidityDevice,
    "power_meter": PowerMeterDevice,
    "solar_panel": SolarPanelDevice,
    "boiler": BoilerDevice,
    "chiller": ChillerDevice,
    "compressed_air": CompressedAirDevice,
    "substation_meter": SubstationMeterDevice,
    "energy_storage": EnergyStorageDevice,
    "gas_flow": GasFlowDevice,
    "water_meter": WaterMeterDevice,
}


def load_config(config_path):
    """加载 JSON 配置文件"""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def provision_devices(iothub_conn_str, device_ids):
    """使用 IoT Hub Service SDK 自动注册设备 (如不存在则创建)

    返回: {device_id: device_connection_string}
    """
    from azure.iot.hub import IoTHubRegistryManager
    from azure.iot.hub.models import Device, AuthenticationMechanism, SymmetricKey
    import urllib.parse

    manager = IoTHubRegistryManager(iothub_conn_str)

    # 从连接字符串中提取 HostName
    parts = dict(item.split("=", 1) for item in iothub_conn_str.split(";"))
    hostname = parts["HostName"]

    device_conn_strings = {}
    for device_id in device_ids:
        try:
            device = manager.get_device(device_id)
            print(f"  [Provision] Device '{device_id}' already exists")
        except Exception:
            device = Device(device_id=device_id)
            device.authentication = AuthenticationMechanism(
                symmetric_key=SymmetricKey(primary_key=None, secondary_key=None),
                type="sas",
            )
            device = manager.create_device_with_sas(
                device_id, primary_key="", secondary_key="", status="enabled"
            )
            print(f"  [Provision] Device '{device_id}' created")

        # 获取设备主密钥并构造连接字符串
        device = manager.get_device(device_id)
        primary_key = device.authentication.symmetric_key.primary_key
        device_conn_strings[device_id] = (
            f"HostName={hostname};DeviceId={device_id};SharedAccessKey={primary_key}"
        )

    return device_conn_strings


# ============================================================================
# 主运行逻辑
# ============================================================================

_stop_event = threading.Event()


def signal_handler(sig, frame):
    print("\n[Simulator] Stopping...")
    _stop_event.set()


def run_single_device(device_id, device_class, sender, interval, count, dry_run):
    """单设备发送循环"""
    device = device_class(device_id)
    sent = 0

    print(f"[{device_id}] Starting ({device.device_type}), interval={interval}s")

    while not _stop_event.is_set():
        if 0 < count <= sent:
            break

        payload = device.generate()
        try:
            sender.send(payload)
            sent += 1
            status = "DRY-RUN" if dry_run else "SENT"
            print(f"[{device_id}] #{sent} {status} | "
                  f"ts={payload.get('ts', payload.get('timestamp', 'N/A'))} | "
                  f"keys={len(payload)} | "
                  f"size={len(json.dumps(payload, ensure_ascii=False))} bytes")
            if dry_run:
                print(json.dumps(payload, indent=2, ensure_ascii=False)[:500])
                print("..." if len(json.dumps(payload, ensure_ascii=False)) > 500 else "")
        except Exception as e:
            print(f"[{device_id}] ERROR: {e}")

        _stop_event.wait(interval)

    print(f"[{device_id}] Stopped. Total sent: {sent}")


def run_multi_device(devices_config, sender_factory, interval, count, dry_run):
    """多设备并发发送"""
    threads = []
    for device_id, device_class in devices_config:
        sender = sender_factory(device_id)
        t = threading.Thread(
            target=run_single_device,
            args=(device_id, device_class, sender, interval, count, dry_run),
            daemon=True,
        )
        threads.append((t, sender))
        t.start()
        time.sleep(0.5)  # 错开启动

    for t, _ in threads:
        t.join()

    for _, sender in threads:
        sender.close()


def main():
    parser = argparse.ArgumentParser(description="IoT Device Simulator for Azure IoT Hub")
    parser.add_argument("--mode", choices=["single", "multi"], default="single",
                        help="single: 单设备模式; multi: 多设备并发 (默认: single)")
    parser.add_argument("--device-id", default="device-wsd-01",
                        help="单设备模式下的设备ID (默认: device-wsd-01)")
    parser.add_argument("--device-type", default=None,
                        help="设备类型: temperature_humidity, power_meter, solar_panel, "
                             "boiler, chiller, compressed_air, substation_meter, "
                             "energy_storage, gas_flow, water_meter")
    parser.add_argument("--interval", type=float, default=10,
                        help="发送间隔 (秒, 默认: 10)")
    parser.add_argument("--count", type=int, default=0,
                        help="发送消息数量 (0=无限, 默认: 0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅生成并打印消息, 不发送到 IoT Hub")
    parser.add_argument("--connection-string", default=None,
                        help="IoT Hub 设备连接字符串 (也可用环境变量 IOTHUB_DEVICE_CONNECTION_STRING)")
    parser.add_argument("--config", default=None,
                        help="多设备模式配置文件路径 (JSON)")
    parser.add_argument("--init-config", action="store_true",
                        help="生成默认配置文件模板 (simulator_config.json)")
    parser.add_argument("--list-devices", action="store_true",
                        help="列出所有预定义设备")

    args = parser.parse_args()

    # 生成配置文件模板
    if args.init_config:
        config_path = Path("simulator_config.json")
        if config_path.exists():
            print(f"配置文件已存在: {config_path}")
            sys.exit(1)
        config_path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        print(f"已生成配置文件: {config_path}")
        print("请编辑 iothub_connection_string 和 devices 列表后运行:")
        print(f"  python {sys.argv[0]} --mode multi --config {config_path}")
        return

    # 列出设备
    if args.list_devices:
        print("预定义设备列表:")
        print(f"{'Device ID':<20} {'Type':<25}")
        print("-" * 45)
        for did, dcls in DEFAULT_DEVICES:
            print(f"{did:<20} {dcls.__name__}")
        print()
        print("可用设备类型:")
        for type_name in TYPE_MAP:
            print(f"  {type_name}")
        return

    signal.signal(signal.SIGINT, signal_handler)

    if args.mode == "single":
        # 确定设备类
        device_class = None
        if args.device_type:
            device_class = TYPE_MAP.get(args.device_type)
            if not device_class:
                print(f"未知设备类型: {args.device_type}")
                print(f"可用类型: {', '.join(TYPE_MAP.keys())}")
                sys.exit(1)
        else:
            # 从预定义列表匹配
            for did, dcls in DEFAULT_DEVICES:
                if did == args.device_id:
                    device_class = dcls
                    break
            if not device_class:
                device_class = TemperatureHumidityDevice
                print(f"[Warning] 未找到 '{args.device_id}' 的预定义模板, 使用温湿度计模板")

        # 创建发送器
        if args.dry_run:
            sender = DryRunSender()
        else:
            conn_str = args.connection_string or os.environ.get("IOTHUB_DEVICE_CONNECTION_STRING")
            if not conn_str:
                print("错误: 请通过 --connection-string 或环境变量 IOTHUB_DEVICE_CONNECTION_STRING 提供连接字符串")
                print("或使用 --dry-run 模式测试")
                sys.exit(1)
            sender = IoTHubSender(conn_str)

        try:
            run_single_device(args.device_id, device_class, sender, args.interval, args.count, args.dry_run)
        finally:
            sender.close()

    elif args.mode == "multi":
        # ------ 多设备模式 ------
        # 优先从配置文件读取; 否则使用默认设备列表
        if args.config:
            config = load_config(args.config)
            iothub_conn_str = config.get("iothub_connection_string", "")
            interval = config.get("send_interval_seconds", args.interval)
            count = config.get("message_count", args.count)
            devices_config = [
                (d["device_id"], TYPE_MAP[d["device_type"]])
                for d in config["devices"]
                if d["device_type"] in TYPE_MAP
            ]
        else:
            iothub_conn_str = args.connection_string or os.environ.get("IOTHUB_CONNECTION_STRING", "")
            interval = args.interval
            count = args.count
            devices_config = DEFAULT_DEVICES

        if args.dry_run:
            def sender_factory(device_id):
                return DryRunSender()
        else:
            if not iothub_conn_str:
                print("错误: 多设备模式需要 IoT Hub 服务连接字符串")
                print()
                print("方式1 - 使用配置文件:")
                print(f"  python {sys.argv[0]} --init-config")
                print(f"  # 编辑 simulator_config.json 填入连接字符串")
                print(f"  python {sys.argv[0]} --mode multi --config simulator_config.json")
                print()
                print("方式2 - 环境变量:")
                print("  set IOTHUB_CONNECTION_STRING=HostName=xxx;SharedAccessKeyName=iothubowner;SharedAccessKey=xxx")
                print(f"  python {sys.argv[0]} --mode multi")
                print()
                print("方式3 - 先用 dry-run 验证:")
                print(f"  python {sys.argv[0]} --mode multi --dry-run --count 2")
                sys.exit(1)

            # 自动注册设备并获取设备连接字符串
            device_ids = [did for did, _ in devices_config]
            print(f"[Provisioning] Registering {len(device_ids)} devices on IoT Hub...")
            try:
                device_conn_strings = provision_devices(iothub_conn_str, device_ids)
            except ImportError:
                print("错误: 多设备自动注册需要 azure-iot-hub 包")
                print("  pip install azure-iot-hub")
                sys.exit(1)
            except Exception as e:
                print(f"错误: 设备注册失败 - {e}")
                sys.exit(1)

            print(f"[Provisioning] All {len(device_conn_strings)} devices ready\n")

            def sender_factory(device_id):
                conn = device_conn_strings.get(device_id)
                if not conn:
                    print(f"[{device_id}] Warning: 无连接字符串, 跳过")
                    return DryRunSender()
                return IoTHubSender(conn)

        run_multi_device(devices_config, sender_factory, interval, count, args.dry_run)


if __name__ == "__main__":
    main()
