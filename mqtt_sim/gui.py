"""
终端模拟软件 (GUI 版)
====================
- 打开/关闭 MQTT 连接 (可编辑服务器地址、端口、账号、密码)
- 温湿度模拟器: 开始/停止开关, JSON 存储, 值变化时上报 MQTT
- Modbus TCP 采集: 开始/停止开关, 读取本小组寄存器区间并上报, 支持写入
- 消息收发: 手动发送任意主题的消息, 自动接收订阅到的消息
  (可与 MQTTX 互测: MQTTX 发消息本程序能收到, 本程序发的 MQTTX 能收到)

运行: python gui.py
"""

import json
import os
import queue
import random
import threading
import time
import tkinter as tk
from tkinter import messagebox, scrolledtext

from paho.mqtt import client as mqtt_client

# 兼容 pymodbus 2.x 与 3.x 的导入方式
try:
    from pymodbus.client import ModbusTcpClient          # pymodbus >= 3.0
except ImportError:
    from pymodbus.client.sync import ModbusTcpClient     # pymodbus 2.x

# ============================ 配置区 ============================
DEFAULT_BROKER = "172.16.4.211"
DEFAULT_PORT = 9783
DEFAULT_USER = "test"
DEFAULT_PWD = "123456"
DEFAULT_TOPICS = "sensor_temphum,modbus/tcp/data"   # 打开连接后自动订阅的主题(逗号分隔)

MODBUS_IP = "192.168.20.59"
MODBUS_PORT = 5502
MODBUS_UNIT = 1                     # 从站地址(站号), 必须显式指定

GROUP_ID = 6                        # 修改为自己的小组编号
GROUP_REG_COUNT = 1                 # 每小组占用寄存器个数(1 = 小组N对应寄存器0x0000+N-1)
REG_START = 0x0000 + (GROUP_ID - 1) * GROUP_REG_COUNT
REG_NUM = GROUP_REG_COUNT

JSON_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sensor_data.json")
MAX_RECORDS = 3                     # JSON 文件保留最近几条温湿度记录
LOOP_INTERVAL = 2                   # 模拟/采集循环间隔(秒)
MODBUS_TIMEOUT = 15                 # 秒: 单次采集超过该时间视为卡死, 自动重置继续采集
# ================================================================

TOPIC_TEMP_HUM = "sensor_temphum"
TOPIC_MODBUS = "modbus/tcp/data"
TOPIC_MODBUS_WRITE = "modbus/tcp/write"
# 控制指令与数据共用 sensor_temphum:
#   MQTTX 发 1 = 开启温湿度模拟, 0 = 停止; 其他内容当作普通数据接收显示


class TerminalApp:
    def __init__(self, root):
        self.root = root
        self.root.title("终端模拟软件")
        self.root.geometry("820x680")
        self.root.minsize(700, 580)

        self.mqtt = None
        self.sensor_enabled = False
        self.modbus_busy = False
        self.modbus_start_ts = 0.0
        self.sensor_data = None
        self.msg_queue = queue.Queue()   # paho/后台线程 -> UI 的消息队列
        self._sent_recent = []           # 最近 3 秒发送的记录, 用于过滤自身消息回显

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self._poll_queue)
        self.root.after(LOOP_INTERVAL * 1000, self._tick)

    def _record_sent(self, topic, payload):
        """记录刚发送的消息, 用于过滤 broker 回显"""
        self._sent_recent.append((time.time(), topic, payload))
        self._sent_recent = [x for x in self._sent_recent if x[0] > time.time() - 3]

    def _publish(self, topic, payload_str):
        """统一发布入口: 发布 + 记录回显过滤"""
        if not self.mqtt:
            return
        self.mqtt.publish(topic, payload_str, qos=1)
        self._record_sent(topic, payload_str)

    # ================== 界面构建 ==================
    def _build_ui(self):
        # ---- 标题栏 ----
        header = tk.Frame(self.root, bg="#2b3a4a", padx=14, pady=8)
        header.pack(fill="x")
        tk.Label(header, text="终端模拟软件", bg="#2b3a4a", fg="white",
                 font=("Microsoft YaHei", 15, "bold")).pack(anchor="w")
        tk.Label(header, text=f"小组 {GROUP_ID} · MQTT {DEFAULT_BROKER}:{DEFAULT_PORT} · "
                              f"Modbus {MODBUS_IP}:{MODBUS_PORT} · 寄存器 "
                              f"0x{REG_START:04X}~0x{REG_START + REG_NUM - 1:04X}",
                 bg="#2b3a4a", fg="#c9d6e3", font=("Microsoft YaHei", 9)).pack(anchor="w")

        # ---- 状态栏 ----
        self.status_var = tk.StringVar(value="● 未连接, 请先点击「打开连接」")
        tk.Label(self.root, textvariable=self.status_var, fg="#888",
                 font=("Microsoft YaHei", 10)).pack(anchor="w", padx=10, pady=(8, 0))

        # ---- MQTT 连接设置 ----
        conn = tk.LabelFrame(self.root, text="①  MQTT 连接设置", padx=10, pady=6)
        conn.pack(fill="x", padx=10, pady=6)
        tk.Label(conn, text="服务器").grid(row=0, column=0, sticky="e")
        self.e_broker = tk.Entry(conn, width=14)
        self.e_broker.insert(0, DEFAULT_BROKER)
        self.e_broker.grid(row=0, column=1, padx=4)
        tk.Label(conn, text="端口").grid(row=0, column=2, sticky="e")
        self.e_port = tk.Entry(conn, width=7)
        self.e_port.insert(0, str(DEFAULT_PORT))
        self.e_port.grid(row=0, column=3, padx=4)
        tk.Label(conn, text="账号").grid(row=0, column=4, sticky="e")
        self.e_user = tk.Entry(conn, width=9)
        self.e_user.insert(0, DEFAULT_USER)
        self.e_user.grid(row=0, column=5, padx=4)
        tk.Label(conn, text="密码").grid(row=0, column=6, sticky="e")
        self.e_pwd = tk.Entry(conn, width=9, show="*")
        self.e_pwd.insert(0, DEFAULT_PWD)
        self.e_pwd.grid(row=0, column=7, padx=4)
        self.btn_open = tk.Button(conn, text="🔌 打开连接", width=11, command=self.mqtt_open)
        self.btn_open.grid(row=0, column=8, padx=6)
        self.btn_close = tk.Button(conn, text="🔌 关闭连接", width=11, command=self.mqtt_close,
                                   state="disabled")
        self.btn_close.grid(row=0, column=9)

        # ---- 功能控制: 温湿度模拟 + Modbus ----
        func = tk.LabelFrame(self.root, text="②  传感器模拟与 Modbus 采集", padx=10, pady=6)
        func.pack(fill="x", padx=10, pady=6)

        # 温湿度模拟
        tk.Label(func, text="温湿度模拟").grid(row=0, column=0, sticky="w", pady=3)
        self.temp_var = tk.StringVar(value="--")
        self.hum_var = tk.StringVar(value="--")
        tk.Label(func, text="温度:").grid(row=0, column=1, sticky="e")
        tk.Label(func, textvariable=self.temp_var, width=6).grid(row=0, column=2, sticky="w")
        tk.Label(func, text="湿度:").grid(row=0, column=3, sticky="e")
        tk.Label(func, textvariable=self.hum_var, width=6).grid(row=0, column=4, sticky="w")
        self.btn_sensor = tk.Button(func, text="🌡 开始模拟", width=11, command=self.sensor_toggle)
        self.btn_sensor.grid(row=0, column=5, padx=(16, 6))

        # Modbus 采集
        tk.Label(func, text="Modbus 采集").grid(row=1, column=0, sticky="w", pady=3)
        self.reg_var = tk.StringVar(value="--")
        tk.Label(func, textvariable=self.reg_var, width=30, anchor="w",
                 fg="#444").grid(row=1, column=1, columnspan=4, sticky="w")
        self.btn_modbus = tk.Button(func, text="📡 采集一次", width=11, command=self.modbus_fetch_once)
        self.btn_modbus.grid(row=1, column=5, padx=(16, 6))

        # Modbus 写入
        tk.Label(func, text="写入寄存器").grid(row=2, column=0, sticky="w", pady=3)
        tk.Label(func, text="地址(如 0x0000):").grid(row=2, column=1, sticky="e")
        self.e_reg_addr = tk.Entry(func, width=10)
        self.e_reg_addr.grid(row=2, column=2, sticky="w")
        tk.Label(func, text="值:").grid(row=2, column=3, sticky="e")
        self.e_reg_val = tk.Entry(func, width=10)
        self.e_reg_val.grid(row=2, column=4, sticky="w")
        tk.Button(func, text="✍ 写入", width=11, command=self.modbus_write_ui).grid(
            row=2, column=5, padx=(16, 6))

        # ---- 消息收发 ----
        msg = tk.LabelFrame(self.root, text="③  消息收发 (与 MQTTX 互测)", padx=10, pady=6)
        msg.pack(fill="x", padx=10, pady=6)
        tk.Label(msg, text="发送主题").grid(row=0, column=0, sticky="e")
        self.e_send_topic = tk.Entry(msg, width=22)
        self.e_send_topic.insert(0, TOPIC_TEMP_HUM)
        self.e_send_topic.grid(row=0, column=1, padx=4)
        tk.Label(msg, text="内容 (JSON)").grid(row=0, column=2, sticky="e")
        self.e_send_payload = tk.Entry(msg, width=38)
        self.e_send_payload.insert(0, '{"温度":25.0,"湿度":60.0,"备注":"测试"}')
        self.e_send_payload.grid(row=0, column=3, padx=4)
        tk.Button(msg, text="📤 发送", width=9, command=self.do_send).grid(row=0, column=4, padx=6)
        tk.Label(msg, text="订阅主题").grid(row=1, column=0, sticky="e", pady=4)
        self.e_sub_topic = tk.Entry(msg, width=22)
        self.e_sub_topic.insert(0, DEFAULT_TOPICS)
        self.e_sub_topic.grid(row=1, column=1, padx=4)
        tk.Button(msg, text="📥 订阅", width=9, command=self.do_subscribe).grid(row=1, column=2, padx=4)
        tk.Label(msg, text="(打开连接后自动订阅; MQTTX 发到这些主题即可收到)",
                 fg="#999", font=("Microsoft YaHei", 8)).grid(row=1, column=3, columnspan=2, sticky="w")

        # ---- 消息日志 ----
        log = tk.LabelFrame(self.root, text="④  消息日志 (绿色=接收 蓝色=发送 灰色=系统)", padx=10, pady=6)
        log.pack(fill="both", expand=True, padx=10, pady=6)
        self.txt_log = scrolledtext.ScrolledText(log, height=14, state="disabled",
                                                 font=("Consolas", 9))
        self.txt_log.pack(fill="both", expand=True)
        self.txt_log.tag_config("sys", foreground="#888888")
        self.txt_log.tag_config("send", foreground="#1a6fc4")
        self.txt_log.tag_config("recv", foreground="#1e8e3e")
        self.txt_log.tag_config("err", foreground="#d93025")
        self.txt_log.tag_config("ok", foreground="#b45309")
        tk.Button(log, text="🗑 清空日志", command=self.clear_log).pack(anchor="e", pady=4)

        # ---- 底部操作提示 ----
        tk.Label(self.root, text="小提示: 先点「打开连接」, 再开始模拟/采集; "
                                 "在 MQTTX 中订阅 sensor/# 即可看到本程序发出的消息",
                 fg="#999", font=("Microsoft YaHei", 8)).pack(anchor="w", padx=12, pady=(0, 6))

    # ================== 日志 ==================
    def log(self, tag, text, color="sys"):
        self.txt_log.configure(state="normal")
        line = f"[{time.strftime('%H:%M:%S')}] {text}\n"
        self.txt_log.insert("end", line, color)
        self.txt_log.see("end")
        self.txt_log.configure(state="disabled")

    def clear_log(self):
        self.txt_log.configure(state="normal")
        self.txt_log.delete("1.0", "end")
        self.txt_log.configure(state="disabled")

    # ================== MQTT 打开/关闭 ==================
    def mqtt_open(self):
        if self.mqtt:
            return
        try:
            client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
            client.username_pw_set(self.e_user.get().strip(), self.e_pwd.get())
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message
            client.connect(self.e_broker.get().strip(),
                           int(self.e_port.get().strip()), keepalive=30)
            client.loop_start()
        except Exception as e:
            messagebox.showerror("连接失败", f"无法连接 MQTT 服务器:\n{e}")
            return
        self.mqtt = client
        # 自动订阅
        topics = [t.strip() for t in self.e_sub_topic.get().split(",") if t.strip()]
        for t in topics:
            client.subscribe(t, qos=1)
        self.btn_open.configure(state="disabled")
        self.btn_close.configure(state="normal")
        self.status_var.set(f"● 已连接 {self.e_broker.get()}:{self.e_port.get()}  (订阅: {', '.join(topics)})")
        self.log("系统", f"[系统] ✅ 已打开连接, 订阅主题: {', '.join(topics)}", "ok")

    def mqtt_close(self):
        if not self.mqtt:
            return
        try:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
        except Exception:
            pass
        self.mqtt = None
        self.btn_open.configure(state="normal")
        self.btn_close.configure(state="disabled")
        self.status_var.set("● 未连接")
        self.log("系统", "[系统] 🔌 已关闭连接", "sys")

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.msg_queue.put(("syslog", f"❌ MQTT 连接失败: {reason_code}", "err"))
        else:
            self.msg_queue.put(("syslog", "✅ MQTT 服务器连接成功", "ok"))

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.msg_queue.put(("syslog", f"⚠️ 连接断开: {reason_code}", "err"))

    def _on_message(self, client, userdata, msg):
        """paho 后台线程回调: 只往队列放, 由 UI 轮询处理"""
        payload = msg.payload.decode(errors="replace")
        # 控制指令: 在 sensor_temphum 上发 1 开启温湿度模拟, 0 停止
        if msg.topic == TOPIC_TEMP_HUM:
            cmd = payload.strip().strip('"').strip("'")
            if cmd == "1":
                self.msg_queue.put(("ctrl", True))
                return
            if cmd == "0":
                self.msg_queue.put(("ctrl", False))
                return
        # 过滤自身消息回显(3 秒内刚发过的 topic+内容)
        now = time.time()
        if any(abs(now - ts) < 3 and t == msg.topic and p == payload
               for ts, t, p in self._sent_recent):
            return
        self.msg_queue.put(("recv", msg.topic, payload))

    # ================== 发送 / 订阅 ==================
    def do_send(self):
        topic = self.e_send_topic.get().strip()
        payload = self.e_send_payload.get()
        if not topic:
            messagebox.showwarning("提示", "请填写发送主题")
            return
        if not self.mqtt:
            messagebox.showwarning("提示", "请先点击「打开连接」")
            return
        info = self.mqtt.publish(topic, payload, qos=1)
        if info.rc == mqtt_client.MQTT_ERR_SUCCESS:
            self._record_sent(topic, payload)
            self.log("发送", f"[发送] [{topic}] {payload}", "send")
        else:
            self.log("发送", f"[发送] [{topic}] 发布失败 rc={info.rc}", "err")

    def do_subscribe(self):
        if not self.mqtt:
            messagebox.showwarning("提示", "请先点击「打开连接」")
            return
        topics = [t.strip() for t in self.e_sub_topic.get().split(",") if t.strip()]
        for t in topics:
            self.mqtt.subscribe(t, qos=1)
        self.log("系统", f"[系统] 📥 已订阅: {', '.join(topics)}", "ok")

    # ================== 温湿度模拟器 ==================
    def sensor_toggle(self):
        if self.sensor_enabled:
            self.sensor_enabled = False
            self.btn_sensor.configure(text="🌡 开始模拟")
            self.log("系统", "[系统] ⏹ 温湿度模拟已停止", "sys")
            return
        if not self.mqtt:
            messagebox.showwarning("提示", "请先点击「打开连接」")
            return
        self._init_json_file()
        self.sensor_data = self._read_sensor_json()
        self.sensor_enabled = True
        self.btn_sensor.configure(text="🌡 停止模拟")
        self.log("系统", f"[系统] ▶ 温湿度模拟已开始 (初始值来自 {JSON_FILE})", "ok")

    def _init_json_file(self):
        """JSON 文件不存在/损坏时, 创建记录格式的默认文件"""
        try:
            with open(JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "records" not in data:
                # 旧格式自动转换: {"temperature":..,"humidity":..} -> {"records":[...]}
                old = {"温度": data.get("temperature", 25.0),
                       "湿度": data.get("humidity", 60.0),
                       "时间": time.strftime("%H:%M:%S")}
                with open(JSON_FILE, "w", encoding="utf-8") as f:
                    json.dump({"records": [old]}, f, indent=2)
        except (FileNotFoundError, json.JSONDecodeError):
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump({"records": []}, f, indent=2)

    def _read_sensor_json(self):
        """返回最新一条记录的 温度/湿度, 作为模拟起点"""
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        recs = data.get("records") if isinstance(data, dict) else None
        if isinstance(recs, list) and recs:
            return {"temperature": recs[0]["温度"], "humidity": recs[0]["湿度"]}
        return {"temperature": 25.0, "humidity": 60.0}

    def _append_sensor_record(self, new_data):
        """追加一条记录, 只保留最近 MAX_RECORDS 条"""
        record = {"温度": new_data["temperature"], "湿度": new_data["humidity"],
                  "时间": time.strftime("%H:%M:%S")}
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

    def _sensor_step(self):
        """模拟小幅波动; 值变化则写 JSON 并上报"""
        old = self.sensor_data
        new = {
            "temperature": round(old["temperature"] + random.uniform(-0.5, 0.5), 1),
            "humidity": round(old["humidity"] + random.uniform(-1.0, 1.0), 1),
        }
        self.sensor_data = new
        self.temp_var.set(f"{new['temperature']} ℃")
        self.hum_var.set(f"{new['humidity']} %")
        if new != old:
            self._append_sensor_record(new)
            payload = {"温度": new["temperature"],
                       "湿度": new["humidity"], "时间": time.strftime("%H:%M:%S")}
            p = json.dumps(payload, ensure_ascii=False)
            self._publish(TOPIC_TEMP_HUM, p)
            self.log("发送", f"[发送] [{TOPIC_TEMP_HUM}] {p}", "send")

    # ================== Modbus 采集 / 写入 ==================
    def modbus_fetch_once(self):
        """手动采集一次: 点一次按钮 = 读一次寄存器 + MQTT 上报一次"""
        if not self.mqtt:
            messagebox.showwarning("提示", "请先点击「打开连接」")
            return
        if self.modbus_busy:
            # 看门狗: 上次采集卡死超过 MODBUS_TIMEOUT 秒则强制重置
            if time.time() - self.modbus_start_ts > MODBUS_TIMEOUT:
                self.modbus_busy = False
                self.log("系统", "⚠️ 上次采集超时, 已自动重置", "err")
            else:
                self.log("系统", "⏳ 上次采集尚未完成, 请稍候", "sys")
                return
        self.modbus_busy = True
        self.modbus_start_ts = time.time()
        threading.Thread(target=self._run_modbus_read, daemon=True).start()
        self.log("系统", f"[系统] 📡 正在采集 ({MODBUS_IP}:{MODBUS_PORT} 站号{MODBUS_UNIT}, "
                         f"寄存器 0x{REG_START:04X}~0x{REG_START + REG_NUM - 1:04X})", "ok")

    def _modbus_worker(self, write=None):
        """后台线程: 读寄存器或写寄存器, 结果通过队列回 UI"""
        client = ModbusTcpClient(host=MODBUS_IP, port=MODBUS_PORT, timeout=3)
        try:
            if not client.connect():
                self.msg_queue.put(("modbus_err", "Modbus TCP 连接失败"))
                return
            if write is not None:
                reg_addr, value = write
                resp = client.write_register(reg_addr, value, unit=MODBUS_UNIT)
                if resp.isError():
                    self.msg_queue.put(("modbus_err", f"写入 0x{reg_addr:04X} 失败: {resp}"))
                else:
                    self.msg_queue.put(("modbus_write_ok", reg_addr, value))
            else:
                resp = client.read_holding_registers(address=REG_START, count=REG_NUM,
                                                     unit=MODBUS_UNIT)
                if resp.isError():
                    self.msg_queue.put(("modbus_err", f"读取失败: {resp}"))
                else:
                    self.msg_queue.put(("modbus_result", resp.registers))
        except Exception as e:
            self.msg_queue.put(("modbus_err", f"Modbus 异常: {e}"))
        finally:
            client.close()

    def modbus_write_ui(self):
        if not self.mqtt:
            messagebox.showwarning("提示", "请先点击「打开连接」")
            return
        try:
            reg_addr = int(self.e_reg_addr.get().strip(), 0)
            value = int(self.e_reg_val.get().strip(), 0)
        except ValueError:
            messagebox.showerror("输入错误", "地址和值必须是整数, 如 0x0003 / 100")
            return
        if not (REG_START <= reg_addr <= REG_START + REG_NUM - 1):
            messagebox.showerror("越界", f"0x{reg_addr:04X} 不在本小组区间 "
                                         f"0x{REG_START:04X}~0x{REG_START + REG_NUM - 1:04X} 内")
            return
        threading.Thread(target=self._modbus_worker,
                         args=((reg_addr, value),), daemon=True).start()

    # ================== UI 轮询 / 定时器 ==================
    def _ctrl_sensor(self, on):
        """根据 MQTTX 控制指令开启/关闭温湿度模拟"""
        if not self.mqtt:
            self.log("系统", "⚠️ 收到控制指令但未连接, 已忽略", "err")
            return
        if on == self.sensor_enabled:
            self.log("系统", f"[控制] 温湿度模拟已处于{'开启' if on else '停止'}状态", "sys")
            return
        self.log("系统", f"[控制] 收到 MQTTX 指令: 温湿度模拟{'开启' if on else '停止'}", "ok")
        self.sensor_toggle()

    def _poll_queue(self):
        """把后台线程(消息/Modbus结果)的内容搬到界面; 任何异常都不允许杀死轮询"""
        while True:
            try:
                kind, *args = self.msg_queue.get_nowait()
            except queue.Empty:
                break
            except Exception as e:
                self.log("系统", f"⚠️ 取消息异常: {e}", "err")
                break
            try:
                if kind == "recv":
                    topic, payload = args
                    self.log("接收", f"[接收] [{topic}] {payload}", "recv")
                elif kind == "syslog":
                    text, color = args
                    self.log("系统", text, color)
                elif kind == "modbus_result":
                    regs = args[0]
                    self.reg_var.set(str(regs))
                    self.log("系统", f"✅ 读取成功: 寄存器 0x{REG_START:04X} = {regs}", "ok")
                    # 手动采集: 点一次采集按钮 = 上报一次
                    payload = {"寄存器": regs, "时间": time.strftime("%H:%M:%S")}
                    p = json.dumps(payload, ensure_ascii=False)
                    self._publish(TOPIC_MODBUS, p)
                    self.log("发送", f"[发送] [{TOPIC_MODBUS}] {p}", "send")
                elif kind == "modbus_write_ok":
                    reg_addr, value = args
                    self.log("系统", f"✍️ 已写入 0x{reg_addr:04X} = {value}", "ok")
                    p = json.dumps({"寄存器地址": f"0x{reg_addr:04X}",
                                    "值": value, "时间": time.strftime("%H:%M:%S")},
                                   ensure_ascii=False)
                    self._publish(TOPIC_MODBUS_WRITE, p)
                elif kind == "modbus_err":
                    self.log("系统", f"⚠️ {args[0]}", "err")
                elif kind == "ctrl":
                    self._ctrl_sensor(args[0])
            except Exception as e:
                self.log("系统", f"⚠️ 处理消息异常[{kind}]: {e}", "err")
        self.root.after(100, self._poll_queue)

    def _tick(self):
        """每 LOOP_INTERVAL 秒执行一次: 温湿度模拟一步 (Modbus 改为手动采集)"""
        if self.sensor_enabled and self.mqtt and self.sensor_data:
            self._sensor_step()
        self.root.after(LOOP_INTERVAL * 1000, self._tick)

    def _run_modbus_read(self):
        try:
            self._modbus_worker()
        finally:
            self.modbus_busy = False

    # ================== 退出 ==================
    def on_close(self):
        self.mqtt_close()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    TerminalApp(root)
    root.mainloop()
