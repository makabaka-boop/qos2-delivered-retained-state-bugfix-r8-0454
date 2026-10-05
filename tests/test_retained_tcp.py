"""真实 TCP 集成测试：--retained 模式下保留状态的端到端语义。

覆盖：
- RETAIN 消息只在完整 QoS2 交付后出现在保留账；pending（等确认）不可见；
- 重复 PUBLISH 的不同 topic/载荷/RETAIN 标志不改变首次受理内容；
- 重复 PUBREL 不重复交付、不改变保留值；PacketId 复用允许新保留消息；
- 零载荷 RETAIN 删除；普通消息不动保留账；
- 四个崩溃点 + 重启后业务账/交换状态/保留账一致；
- 接管、CleanSession 清理、重启不破坏保留账；
- retained_query CLI 的 $ 系统主题边界与空层/末尾 # 行为。
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from mqtt_inbox import framing
from mqtt_inbox.retained import query_retained
from mqtt_inbox.retained import RetainedStorage

from tests._peer import MqttPeer
from tests._server import ServerProcess, free_port


class RetainedTcpTestBase(unittest.TestCase):

    crash = ""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "inbox.db")
        self.port = free_port()
        self.server = ServerProcess(
            self.db_path, crash=self.crash, port=self.port, retained=True
        )
        self.server.start()
        self.peers: list[MqttPeer] = []

    def tearDown(self):
        for p in self.peers:
            p.close()
        if getattr(self, "server", None):
            self.server.terminate()

    def peer(self) -> MqttPeer:
        p = MqttPeer("127.0.0.1", self.server.port)
        self.peers.append(p)
        return p

    def restart(self, crash: str | None = None) -> None:
        self.server.terminate()
        for p in self.peers:
            p.close()
        self.peers.clear()
        self.server = ServerProcess(
            self.db_path,
            crash=crash if crash is not None else self.crash,
            port=self.port,
            retained=True,
        )
        self.server.start()

    def db(self) -> RetainedStorage:
        s = RetainedStorage(self.db_path)
        self.addCleanup(s.close)
        return s

    def deliver(self, p, pid, topic, payload, *, retain=True, dup=False):
        p.publish(topic, payload, pid, retain=retain, dup=dup)
        p.expect_pubrec(pid)
        p.pubrel(pid)
        p.expect_pubcomp(pid)

    def retained_topics(self, topic_filter="#"):
        return [r["topic"] for r in query_retained(self.db_path, topic_filter)]


class RetainedVisibilityTests(RetainedTcpTestBase):

    def test_pending_message_not_visible_in_query(self):
        p = self.peer()
        sp, code = p.connect("dev", clean=False)
        self.assertEqual(code, 0)
        # 先放一条已交付的保留值
        self.deliver(p, 1, "t/v", b"old")
        self.assertEqual(self.retained_topics("t/v"), ["t/v"])

        # 新 PUBLISH 只到 PUBREC：交换未完成，查询必须仍显示旧值
        p.publish("t/v", b"new", 2, retain=True)
        p.expect_pubrec(2)
        rows = query_retained(self.db_path, "t/v")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload_hex"], b"old".hex())

        # 零载荷删除停在 pending：旧值不得被提前删掉
        p.publish("t/v", b"", 3, retain=True)
        p.expect_pubrec(3)
        self.assertEqual(self.retained_topics("t/v"), ["t/v"])

        # 完成 2 之后显示新值；完成 3（零载荷）后删除
        p.pubrel(2)
        p.expect_pubcomp(2)
        rows = query_retained(self.db_path, "t/v")
        self.assertEqual(rows[0]["payload_hex"], b"new".hex())
        p.pubrel(3)
        p.expect_pubcomp(3)
        self.assertEqual(self.retained_topics("t/v"), [])

    def test_normal_message_never_updates_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 1, "t", b"kept", retain=True)
        self.deliver(p, 2, "t", b"plain", retain=False)
        rows = query_retained(self.db_path, "t")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload_hex"], b"kept".hex())

    def test_zero_payload_deletes_on_delivery(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 1, "t", b"v")
        self.deliver(p, 2, "t", b"")
        self.assertEqual(self.retained_topics("#"), [])
        # 业务账仍有两条（删除也是一次交付）
        self.assertEqual(len(self.db().query_inbox("dev")), 2)

    def test_repeated_publish_with_changed_fields_uses_first_accepted(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("orig/topic", b"first", 10, retain=True)
        p.expect_pubrec(10)
        # PUBREC 丢失重传：改 topic、载荷、RETAIN 标志，都必须被忽略
        p.publish("other/topic", b"second", 10, retain=False, dup=True)
        p.expect_pubrec(10)
        p.pubrel(10)
        p.expect_pubcomp(10)
        rows = query_retained(self.db_path, "#")
        self.assertEqual([r["topic"] for r in rows], ["orig/topic"])
        self.assertEqual(rows[0]["payload_hex"], b"first".hex())
        self.assertEqual(self.db().query_inbox("dev")[0].topic, "orig/topic")

    def test_repeated_pubrel_keeps_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 11, "t", b"v")
        p.pubrel(11)
        p.expect_pubcomp(11)
        self.assertEqual(len(self.db().query_inbox("dev")), 1)
        rows = query_retained(self.db_path, "t")
        self.assertEqual(rows[0]["payload_hex"], b"v".hex())

    def test_packet_id_reuse_new_message_replaces(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 20, "t", b"v1")
        # 同编号新消息：零载荷 RETAIN，交付后删除旧保留值
        self.deliver(p, 20, "t", b"")
        self.assertEqual(self.retained_topics("#"), [])
        self.assertEqual(len(self.db().query_inbox("dev")), 2)
        # 编号再次复用：普通消息不动（不存在的）保留账；之后再来保留消息
        self.deliver(p, 20, "t", b"plain", retain=False)
        self.assertEqual(self.retained_topics("#"), [])
        self.deliver(p, 20, "t", b"v2")
        self.assertEqual(
            query_retained(self.db_path, "t")[0]["payload_hex"], b"v2".hex()
        )

    def test_retained_accepted_only_in_retained_mode_flag(self):
        # 本套件全部以 --retained 启动：RETAIN PUBLISH 正常拿到 PUBREC
        p = self.peer()
        p.connect("dev", clean=True)
        p.publish("t", b"x", 1, retain=True)
        p.expect_pubrec(1)
        p.pubrel(1)
        p.expect_pubcomp(1)


class RetainedLedgerConsistencyTests(RetainedTcpTestBase):

    def test_inbox_id_points_at_business_ledger(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 1, "t", b"v")
        db = self.db()
        row = query_retained(self.db_path, "t")[0]
        item = next(i for i in db.query_inbox("dev") if i.id == row["inbox_id"])
        self.assertEqual(item.topic, "t")
        self.assertEqual(item.payload, b"v")
        self.assertEqual(item.packet_id, 1)

    def test_takeover_preserves_retained(self):
        old = self.peer()
        old.connect("dev", clean=False)
        self.deliver(old, 1, "t", b"v")
        new = self.peer()
        sp, _ = new.connect("dev", clean=False)
        self.assertTrue(sp)
        self.assertTrue(old.expect_closed(timeout=5))
        # 接管后保留账完好，新连接可继续更新
        self.deliver(new, 2, "t2", b"w")
        self.assertEqual(
            sorted(self.retained_topics("#")), sorted(["t", "t2"])
        )

    def test_clean1_clears_session_but_not_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("pend", b"x", 5, retain=True)
        p.expect_pubrec(5)
        self.deliver(p, 1, "kept", b"v")

        p2 = self.peer()
        sp, code = p2.connect("dev", clean=True)
        self.assertEqual(code, 0)
        self.assertFalse(sp)
        db = self.db()
        # 未完成交换被清理，保留账和业务账保留
        self.assertIsNone(db.flow_state("dev", 5))
        self.assertEqual(self.retained_topics("#"), ["kept"])
        self.assertEqual(db.inbox_count("dev", 1), 1)

    def test_restart_preserves_retained_and_unfinished_does_not_leak(self):
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 1, "t", b"v")
        p.publish("pend", b"later", 2, retain=True)
        p.expect_pubrec(2)
        p.close()

        self.restart(crash="")
        self.assertEqual(self.retained_topics("#"), ["t"])
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        # 重传完成 pending 交换后，保留值才出现
        p2.publish("pend", b"later", 2, retain=True, dup=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)
        p2.expect_pubcomp(2)
        self.assertEqual(sorted(self.retained_topics("#")), ["pend", "t"])


class RetainedCrashTests(RetainedTcpTestBase):
    """四个崩溃点：重启后三账（inbox / qos2_flows / retained）必须一致。"""

    def _kill_and_restart(self):
        self.assertTrue(self.peers[0].expect_closed(timeout=8))
        proc = self.server.proc
        self.assertEqual(proc.wait(), 137)
        for stream in (proc.stdout, proc.stderr):
            try:
                stream.close()
            except OSError:
                pass
        self.server.proc = None
        self.restart(crash="")

    def test_crash_before_pending_commit_leaves_nothing(self):
        self.server.terminate()
        self.server = ServerProcess(
            self.db_path, crash="before_pending_commit",
            port=self.port, retained=True
        )
        self.server.start()
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"v", 1, retain=True)
        self._kill_and_restart()

        db = self.db()
        self.assertIsNone(db.flow_state("dev", 1))
        self.assertEqual(db.inbox_count("dev", 1), 0)
        self.assertEqual(self.retained_topics("#"), [])
        # 重放 -> 正常交付一次
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        self.deliver(p2, 1, "t", b"v")
        self.assertEqual(self.retained_topics("#"), ["t"])

    def test_crash_after_pending_commit_no_retained_before_delivery(self):
        self.server.terminate()
        self.server = ServerProcess(
            self.db_path, crash="after_pending_commit",
            port=self.port, retained=True
        )
        self.server.start()
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"v", 1, retain=True)
        self._kill_and_restart()

        db = self.db()
        self.assertEqual(db.flow_state("dev", 1), "pending")
        self.assertEqual(db.inbox_count("dev", 1), 0)
        # pending 已落盘但未交付：保留账必须为空
        self.assertEqual(self.retained_topics("#"), [])

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("t", b"v", 1, retain=True, dup=True)
        p2.expect_pubrec(1)
        p2.pubrel(1)
        p2.expect_pubcomp(1)
        self.assertEqual(db.inbox_count("dev", 1), 1)
        self.assertEqual(self.retained_topics("#"), ["t"])

    def test_crash_before_pubrel_commit_atomic_rollback(self):
        # 阶段一：无崩溃注入，先建立保留值 t=old
        p = self.peer()
        p.connect("dev", clean=False)
        self.deliver(p, 1, "t", b"old")
        self.assertEqual(self.retained_topics("#"), ["t"])
        p.close()

        # 阶段二：重启并注入 before_pubrel_commit，用零载荷 RETAIN 删除 t
        self.restart(crash="before_pubrel_commit")
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("t", b"", 2, retain=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)
        self._kill_and_restart()

        db = self.db()
        # 交付事务整体回滚：业务账无新增、交换仍 pending、保留值仍是 old
        self.assertEqual(db.flow_state("dev", 2), "pending")
        self.assertEqual(db.inbox_count("dev", 2), 0)
        rows = query_retained(self.db_path, "t")
        self.assertEqual(rows[0]["payload_hex"], b"old".hex())

        # 重放 PUBREL：三账原子推进，t 被删除
        p3 = self.peer()
        sp, _ = p3.connect("dev", clean=False)
        self.assertTrue(sp)
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertEqual(db.flow_state("dev", 2), "done")
        self.assertEqual(db.inbox_count("dev", 2), 1)
        self.assertEqual(self.retained_topics("#"), [])
        # 再重放 PUBREL：幂等
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertEqual(db.inbox_count("dev", 2), 1)

    def test_crash_after_pubrel_commit_all_three_ledgers_durable(self):
        self.server.terminate()
        self.server = ServerProcess(
            self.db_path, crash="after_pubrel_commit",
            port=self.port, retained=True
        )
        self.server.start()
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"v", 1, retain=True)
        p.expect_pubrec(1)
        p.pubrel(1)
        self._kill_and_restart()

        db = self.db()
        self.assertEqual(db.flow_state("dev", 1), "done")
        self.assertEqual(db.inbox_count("dev", 1), 1)
        self.assertEqual(self.retained_topics("#"), ["t"])

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.pubrel(1)
        p2.expect_pubcomp(1)
        self.assertEqual(db.inbox_count("dev", 1), 1)
        self.assertEqual(self.retained_topics("#"), ["t"])


class RetainedQueryCliTests(RetainedTcpTestBase):

    def _run_cli(self, topic_filter):
        env = os.environ.copy()
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "mqtt_inbox.retained_query",
                "--db",
                self.db_path,
                "--filter",
                topic_filter,
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_cli_wildcards_and_system_boundary(self):
        p = self.peer()
        p.connect("dev", clean=False)
        pid = 1
        for topic, payload in [
            ("a/b", b"1"),
            ("a//b", b"2"),
            ("a/", b"3"),
            ("$SYS/x", b"4"),
        ]:
            self.deliver(p, pid, topic, payload)
            pid += 1

        # # 不混入系统主题
        self.assertEqual(
            sorted(r["topic"] for r in self._run_cli("#")),
            sorted(["a/b", "a//b", "a/"]),
        )
        # + 空层匹配
        self.assertEqual(
            [r["topic"] for r in self._run_cli("a/+/b")], ["a//b"]
        )
        # 末尾 # 零层
        self.assertEqual(
            sorted(r["topic"] for r in self._run_cli("a/#")),
            sorted(["a/b", "a//b", "a/"]),
        )
        # 系统主题仅显式可达
        self.assertEqual(
            [r["topic"] for r in self._run_cli("$SYS/#")], ["$SYS/x"]
        )
        self.assertEqual(self._run_cli("+/x"), [])

    def test_cli_invalid_filter_exit_nonzero(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "mqtt_inbox.retained_query",
                "--db",
                self.db_path,
                "--filter",
                "a/+b",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
        self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
