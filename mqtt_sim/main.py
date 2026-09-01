"""
终端模拟软件
============
功能1: 温湿度传感器模拟器
    - 传感器值存于 JSON 文件 sensor_data.json
    - 每轮模拟小幅波动, 数值发生变化时及时通过 MQTT 上报

功能2: Modbus TCP 数据采集与写入
    - Modbus TCP 从站: 192.168.20.59:5502


    - 寄存器区 0x0000~0x0009 (10 个保持寄存器)
    - 每个小组只读写自己小组对应的寄存器区间, 避免与其他小组冲突
    - 采集到的数据上报到 MQTT 服务器

MQTT 服务器: 172.16.4.211:9783  账号 test / 密码 123456

用法:
    python main.py                    # 同时运行两个功能(主循环)
    python main.py write 0x0003 100   # 向本小组寄存器写入一个值后退出
"""

import argparse
import json
import random
import sys
import time

# 兼容 Windows GBK 控制台: 输出统一转 UTF-8, 打印 emoji 时不崩溃
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from paho.mqtt import client as mqtt_client
# 兼容 pymodbus 2.x 与 3.x 的导入方式
try:
    from pymodbus.client import ModbusTcpClient          # pymodbus >= 3.0
except ImportError:
    from pymodbus.client.sync import ModbusTcpClient     # pymodbus 2.x

# ============================ 配置区(所有参数在这里改) ============================
MQTT_BROKER = "172.16.4.211"
MQTT_PORT = 9783
MQTT_USER = "test"
MQTT_PWD = "123456"
MQTT_QOS = 1                       # 0/1/2, 1 表示至少送达一次, 更可靠

TOPIC_TEMP_HUM = "sensor_temphum"      # 温湿度上报主题
TOPIC_MODBUS = "modbus/tcp/data"       # modbus 数据上报主题
TOPIC_MODBUS_WRITE = "modbus/tcp/write"  # modbus 写入记录主题

JSON_FILE = "sensor_data.json"     # 温湿度 JSON 存储文件
MAX_RECORDS = 3                    # JSON 文件保留最近几条温湿度记录

MODBUS_IP = "192.168.20.59"
MODBUS_PORT = 5502
MODBUS_UNIT = 1                    # 从站地址(站号), 必须显式指定, 否则部分版本默认站号不为1

# ---- 小组寄存器分配(避免小组间冲突) ----
# 从站寄存器总范围 0x0000 ~ 0x0009 共 10 个
# 每个小组只读写自己小组对应的连续区间:
#   小组1: 0x0000 ~ 0x0004   小组2: 0x0005 ~ 0x0009
GROUP_ID = 6                       # 修改为自己的小组编号
GROUP_REG_COUNT = 1                # 每小组占用寄存器个数(1 = 小组N对应寄存器0x0000+N-1)
REG_START = 0x0000 + (GROUP_ID - 1) * GROUP_REG_COUNT
REG_NUM = GROUP_REG_COUNT

LOOP_INTERVAL = 2                  # 主循环间隔(秒)
# ===============================================================================



# ---------------- 1. MQTT 客户端 ----------------
def mqtt_init():
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PWD)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            print(f"❌ MQTT 连接失败: {reason_code}")
        else:
            print("✅ MQTT 服务器连接成功")

    def on_disconnect(c, userdata, disconnect_flags, reason_code, properties):
        print(f"⚠️ MQTT 断开连接: {reason_code}, 自动重连中...")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # 启动时尝试连接, 失败则重试
    for attempt in range(1, 6):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT)
            break
        except Exception as e:
            print(f"⚠️ MQTT 连接失败(第{attempt}次): {e}")
            time.sleep(2)
    else:
        print("❌ 无法连接 MQTT 服务器, 程序退出")
        sys.exit(1)

    client.loop_start()  # 后台线程处理 mqtt 收发
    return client


def mqtt_publish(client, topic, payload_dict):
    """封装发布消息函数, 返回是否成功"""
    payload = json.dumps(payload_dict, ensure_ascii=False)
    info = client.publish(topic, payload, qos=MQTT_QOS)
    if info.rc == mqtt_client.MQTT_ERR_SUCCESS:
        print(f"📤 [{topic}] {payload}")
        return True
    print(f"⚠️ 发布失败 [{topic}] rc={info.rc}")
    return False


# ---------------- 2. 温湿度模拟器 (JSON 读写) ----------------
def init_json_file():
    """JSON 文件不存在/损坏时, 创建记录格式的默认文件"""
    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "records" not in data:
            # 旧格式自动转换
            old = {"温度": data.get("temperature", 25.0),
                   "湿度": data.get("humidity", 60.0),
                   "时间": time.strftime("%H:%M:%S")}
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump({"records": [old]}, f, indent=2)
            print(f"📄 {JSON_FILE} 已转换为记录格式")
    except FileNotFoundError:
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump({"records": []}, f, indent=2)
        print(f"📄 新建 {JSON_FILE}")
    except json.JSONDecodeError:
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump({"records": []}, f, indent=2)
        print(f"⚠️ {JSON_FILE} 损坏, 已重建")


def read_sensor_json():
    """返回最新一条记录的 温度/湿度, 作为模拟起点"""
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    recs = data.get("records") if isinstance(data, dict) else None
    if isinstance(recs, list) and recs:
        return {"temperature": recs[0]["温度"], "humidity": recs[0]["湿度"]}
    return {"temperature": 25.0, "humidity": 60.0}


def append_sensor_record(temp, hum):
    """追加一条记录, 只保留最近 MAX_RECORDS 条"""
    record = {"温度": temp, "湿度": hum, "时间": time.strftime("%H:%M:%S")}
    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    recs = data.get("records") if isinstance(data, dict) else None
    if not isinstance(recs, list):
        recs = []
    recs.insert(0, record)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump({"records": recs[:MAX_RECORDS]}, f, indent=2)


def sensor_simulator(mqtt, old_data):
    """模拟温湿度小幅波动, 值发生变化时写 JSON 并上报 MQTT"""
    new_data = {
        "temperature": round(old_data["temperature"] + random.uniform(-0.5, 0.5), 1),
        "humidity": round(old_data["humidity"] + random.uniform(-1.0, 1.0), 1),
    }
    if new_data != old_data:
        print("🌡️ 温湿度发生变化")
        append_sensor_record(new_data["temperature"], new_data["humidity"])
        payload = {
            "温度": new_data["temperature"],
            "湿度": new_data["humidity"],
            "时间": time.strftime("%H:%M:%S"),
        }
        mqtt_publish(mqtt, TOPIC_TEMP_HUM, payload)
    return new_data


# ---------------- 3. Modbus TCP 采集与写入 ----------------
def modbus_read_regs(mqtt):
    """通过 Modbus TCP 读取本小组寄存器并上报 MQTT"""
    client = ModbusTcpClient(host=MODBUS_IP, port=MODBUS_PORT, timeout=3)
    if not client.connect():
        print("❌ Modbus TCP 连接失败, 请检查 IP/端口/网络")
        return None
    try:
        resp = client.read_holding_registers(address=REG_START, count=REG_NUM,
                                             unit=MODBUS_UNIT)
        if resp.isError():
            print(f"⚠️ Modbus 读取异常: {resp}")
            return None
        regs = resp.registers
        payload = {
            "寄存器": regs,
            "时间": time.strftime("%H:%M:%S"),
        }
        mqtt_publish(mqtt, TOPIC_MODBUS, payload)
        return regs
    except Exception as e:
        print(f"⚠️ Modbus 异常: {e}")
        return None
    finally:
        client.close()


def modbus_write_reg(mqtt, reg_addr, value):
    """向本小组寄存器写入一个值 (地址必须在自己小组区间内)"""
    if not (REG_START <= reg_addr <= REG_START + REG_NUM - 1):
        print(f"❌ 寄存器 0x{reg_addr:04X} 不在本小组区间"
              f" 0x{REG_START:04X}~0x{REG_START + REG_NUM - 1:04X} 内")
        return False
    client = ModbusTcpClient(host=MODBUS_IP, port=MODBUS_PORT, timeout=3)
    if not client.connect():
        print("❌ Modbus TCP 连接失败")
        return False
    try:
        resp = client.write_register(reg_addr, value, unit=MODBUS_UNIT)
        if resp.isError():
            print(f"⚠️ 写入失败: {resp}")
            return False
        print(f"✍️ 已写入 0x{reg_addr:04X} = {value}")
        mqtt_publish(mqtt, TOPIC_MODBUS_WRITE, {
            "寄存器地址": f"0x{reg_addr:04X}",
            "值": value,
            "时间": time.strftime("%H:%M:%S"),
        })
        return True
    except Exception as e:
        print(f"⚠️ 写入异常: {e}")
        return False
    finally:
        client.close()


# ---------------- 4. 主循环 ----------------
def main_loop(mqtt):
    init_json_file()
    sensor_data = read_sensor_json()
    # 启动时上报一次当前温湿度, 让服务器立即拿到初始值
    mqtt_publish(mqtt, TOPIC_TEMP_HUM, {
        "温度": sensor_data["temperature"],
        "湿度": sensor_data["humidity"],
        "时间": time.strftime("%H:%M:%S"),
    })
    print(f"🚀 程序启动, 每 {LOOP_INTERVAL} 秒循环一次 (Ctrl+C 退出)")
    try:
        while True:
            sensor_data = sensor_simulator(mqtt, sensor_data)  # 功能1: 温湿度模拟
            modbus_read_regs(mqtt)                             # 功能2: Modbus 采集上报
            time.sleep(LOOP_INTERVAL)
    except KeyboardInterrupt:
        print("\n🛑 程序退出")
    finally:
        mqtt.loop_stop()
        mqtt.disconnect()


if __name__ == "__main__":
    # 校验小组寄存器区间不越界
    if REG_START + REG_NUM - 1 > 0x0009:
        print("❌ 小组寄存器区间超出 0x0000~0x0009, 请检查 GROUP_ID / GROUP_REG_COUNT")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="终端模拟软件")
    parser.add_argument("cmd", nargs="?", default="run",
                        help="run=运行主循环; write=写寄存器")
    parser.add_argument("addr", nargs="?", help="寄存器地址, 如 0x0003 或 3")
    parser.add_argument("value", nargs="?", type=int, help="要写入的值")
    args = parser.parse_args()

    mqtt = mqtt_init()

    if args.cmd == "write":
        if args.addr is None or args.value is None:
            print("用法: python main.py write <寄存器地址> <值>")
            print("例:   python main.py write 0x0003 100")
            sys.exit(1)
        modbus_write_reg(mqtt, int(args.addr, 0), args.value)
        mqtt.loop_stop()
        mqtt.disconnect()
    else:
        main_loop(mqtt)
