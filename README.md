# demo

物联网实验项目：**终端模拟软件**

基于 Python 的终端模拟软件，实现两个功能：

1. **温湿度传感器模拟器**：传感器值存储于 JSON 文件（保留最近 3 条记录），值发生变化时通过 MQTT 及时上报，支持 MQTTX 远程控制（发 `1` 开启、`0` 停止）
2. **Modbus TCP 数据采集**：通过 Modbus TCP 从 PLC 从站采集寄存器数据并上报到 MQTT 服务器，每个小组只读写自己小组对应的寄存器区间，避免冲突

## 项目结构

```
mqtt_sim/
├── gui.py               # 图形界面版（推荐）：连接开关、消息收发、温湿度模拟、Modbus 采集与写入
├── main.py              # 命令行版：自动循环运行两个功能
├── requirements.txt     # 依赖清单
└── README.md            # 详细使用说明
```

## 快速开始

```bash
cd mqtt_sim
pip install -r requirements.txt
python gui.py            # 图形界面
```

详细配置说明、消息格式、与 MQTTX 互测方法见 [mqtt_sim/README.md](mqtt_sim/README.md)。
