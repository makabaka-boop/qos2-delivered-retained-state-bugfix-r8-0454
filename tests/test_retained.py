"""保留状态（--retained）测试：真实 TCP + 真实子进程 + 存储层与过滤器单元测试。

覆盖需求：
- 保留状态只在 QoS2 交换成功交付（PUBREL 入账事务）后可见：pending 期间
  查询既看不到新值，也不会提前删除旧值；
- RETAIN 零载荷删除 topic；普通（RETAIN=0）消息不更新保留账；
- 重复 PUBLISH（不同 topic/载荷/RETAIN 标志）一律以首次受理为准；
- 重复 PUBREL 不重复交付、不改变保留值；PacketId 完成后可复用；
- 业务账、交换完成状态、保留账在同一持久边界一致（四个崩溃点 × 重启重放）；
- 接管、CleanSession=1 清理协议会话、服务重启（含 --retained 开关切换）
  都不破坏已有保留账；
- 过滤器：空层、末尾 # 匹配零/多层、$ 系统主题边界、非法过滤器报错；
- retained_query CLI 真实子进程只读查询。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from mqtt_inbox.retained import (
    RetainedStorage,
    filter_levels,
    matches,
    query_retained,
)

from tests._peer import MqttPeer
from tests._server import ServerProcess, free_port


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def retained_map(path: str) -> dict[str, tuple[bytes, int]]:
    """直接读 retained 表（含 $ 主题；query_retained('#') 会按规则排除它们）。"""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT topic, payload, inbox_id FROM retained"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    finally:
        conn.close()
    return {r[0]: (bytes(r[1]), r[2]) for r in rows}


def deliver(peer: MqttPeer, topic: str, payload: bytes, pid: int, **kw) -> None:
    peer.publish(topic, payload, pid, **kw)
    peer.expect_pubrec(pid)
    peer.pubrel(pid)
    peer.expect_pubcomp(pid)


# ---------------------------------------------------------------------------
# 过滤器纯函数
# ---------------------------------------------------------------------------


class FilterLevelTests(unittest.TestCase):
    def test_valid_filters_split(self):
        self.assertEqual(filter_levels("a/b/c"), ["a", "b", "c"])
        self.assertEqual(filter_levels("a//c"), ["a", "", "c"])  # 空层有效
        self.assertEqual(filter_levels("a/"), ["a", ""])
        self.assertEqual(filter_levels("/a"), ["", "a"])
        self.assertEqual(filter_levels("+"), ["+"])
        self.assertEqual(filter_levels("a/+/c/#"), ["a", "+", "c", "#"])

    def test_invalid_filters(self):
        for bad in ("", "a+b", "a#", "a/#/b", "a/#/", "#/a", "a/b#", "x\x00y"):
            with self.assertRaises(ValueError, msg=bad):
                filter_levels(bad)

    def test_plus_matches_exactly_one_level_including_empty(self):
        self.assertTrue(matches("a/b/c", filter_levels("a/+/c")))
        self.assertTrue(matches("a//c", filter_levels("a/+/c")))
        self.assertFalse(matches("a/b", filter_levels("a/+/c")))
        self.assertFalse(matches("a/x/y/c", filter_levels("a/+/c")))
        self.assertTrue(matches("x/", filter_levels("x/+")))  # + 匹配空层
        self.assertFalse(matches("x", filter_levels("x/+")))

    def test_hash_matches_zero_or_more_levels(self):
        lv = filter_levels("a/#")
        self.assertTrue(matches("a", lv))  # 零层
        self.assertTrue(matches("a/b", lv))
        self.assertTrue(matches("a/b/c", lv))
        self.assertTrue(matches("a//c", lv))
        self.assertFalse(matches("b", lv))
        self.assertTrue(matches("x/", filter_levels("x/#")))

    def test_hash_only_matches_all_non_system(self):
        self.assertTrue(matches("anything", filter_levels("#")))
        self.assertTrue(matches("a/b/c", filter_levels("#")))
        self.assertFalse(matches("$SYS/x", filter_levels("#")))
        self.assertFalse(matches("$things/x", filter_levels("#")))

    def test_system_topic_wildcard_boundary(self):
        # 首层通配：绝不匹配 $ 主题
        self.assertFalse(matches("$SYS/load", filter_levels("+")))
        self.assertFalse(matches("$SYS/load", filter_levels("+/load")))
        self.assertFalse(matches("$SYS/load", filter_levels("+/+")))
        self.assertFalse(matches("$SYS/load", filter_levels("#")))
        # 过滤器显式以 $ 开头：按普通规则匹配
        self.assertTrue(matches("$SYS/load", filter_levels("$SYS/+")))
        self.assertTrue(matches("$SYS/load", filter_levels("$SYS/#")))
        self.assertTrue(matches("$SYS", filter_levels("$SYS/#")))  # # 匹配零层
        self.assertFalse(matches("$things/x", filter_levels("$SYS/#")))
        self.assertTrue(matches("$SYS/a/b", filter_levels("$SYS/#")))
        self.assertTrue(matches("$SYS/load", filter_levels("$SYS/load")))


# ---------------------------------------------------------------------------
# 存储层（不经 TCP，快速覆盖事务语义）
# ---------------------------------------------------------------------------


class RetainedStorageTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.db = RetainedStorage(self.path)
        self.epoch, _ = self.db.begin_connect("dev", clean=False)

    def tearDown(self):
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def _retained(self):
        return {
            r[0]: (bytes(r[1]), r[2])
            for r in self.db.conn.execute(
                "SELECT topic, payload, inbox_id FROM retained"
            )
        }

    def test_pending_does_not_touch_retained(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v", False, retain=True)
        self.assertEqual(self._retained(), {})  # PUBREC 前/后均不可见

    def test_delivery_upserts_with_inbox_id(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v1", False, retain=True)
        self.assertEqual(
            self.db.finish_on_pubrel("dev", self.epoch, 1), "delivered"
        )
        row = self._retained()["rt"]
        self.assertEqual(row[0], b"v1")
        inbox_id = self.db.conn.execute(
            "SELECT id FROM inbox WHERE client_id='dev' AND packet_id=1"
        ).fetchone()[0]
        self.assertEqual(row[1], inbox_id)  # 保留值指向产生它的业务账行

    def test_zero_payload_deletes(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"old", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 1)
        self.db.save_pending("dev", self.epoch, 2, "rt", b"", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 2)
        self.assertEqual(self._retained(), {})

    def test_normal_message_never_touches_retained(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"old", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 1)
        self.db.save_pending("dev", self.epoch, 2, "rt", b"new", False, retain=False)
        self.db.finish_on_pubrel("dev", self.epoch, 2)
        self.assertEqual(self._retained()["rt"][0], b"old")  # 普通消息不覆盖

    def test_dup_keeps_first_accepted_topic_payload_retain(self):
        # 首次受理：topic=rt, v1, RETAIN=1
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v1", False, retain=True)
        # 重传换成别的 topic/载荷/RETAIN 标志：全部忽略
        self.db.save_pending("dev", self.epoch, 1, "other", b"v2", True, retain=False)
        self.assertEqual(
            self.db.finish_on_pubrel("dev", self.epoch, 1), "delivered"
        )
        self.assertEqual(set(self._retained()), {"rt"})
        self.assertEqual(self._retained()["rt"][0], b"v1")
        self.assertEqual(self.db.inbox_count("dev", 1), 1)

    def test_first_non_retained_then_dup_retained_is_ignored(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v", False, retain=False)
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v", True, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 1)
        self.assertEqual(self._retained(), {})

    def test_repeat_pubrel_does_not_change_retained(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"v1", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 1)
        # 先把保留值改成新交付的 v2（另一 PacketId），再重放旧 PUBREL
        self.db.save_pending("dev", self.epoch, 2, "rt", b"v2", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 2)
        self.assertEqual(
            self.db.finish_on_pubrel("dev", self.epoch, 1), "repeat"
        )
        self.assertEqual(self._retained()["rt"][0], b"v2")
        self.assertEqual(self.db.inbox_count("dev", 1), 1)

    def test_clean1_wipe_and_plain_reopen_preserve_retained(self):
        self.db.save_pending("dev", self.epoch, 1, "rt", b"keep", False, retain=True)
        self.db.finish_on_pubrel("dev", self.epoch, 1)
        # CleanSession=1 清协议交换状态，不动保留账
        self.db.begin_connect("dev", clean=True)
        self.assertEqual(self._retained()["rt"][0], b"keep")
        self.db.close()
        # 不以 --retained 启动（基类 Storage）也不得删除/破坏保留账
        plain = RetainedStorage(self.path)
        plain.begin_connect("dev", clean=True)
        plain.close()
        checker = RetainedStorage(self.path)
        try:
            self.assertEqual(
                bytes(
                    checker.conn.execute(
                        "SELECT payload FROM retained WHERE topic='rt'"
                    ).fetchone()[0]
                ),
                b"keep",
            )
        finally:
            checker.close()

    def test_old_db_without_retain_column_migrates(self):
        # 用旧结构手工建库（qos2_flows 无 retain 列），打开必须自动迁移
        self.db.close()
        os.unlink(self.path)
        for suffix in ("-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass
        conn = sqlite3.connect(self.path)
        conn.executescript(
            """
            CREATE TABLE sessions(client_id TEXT PRIMARY KEY, clean INTEGER,
                epoch INTEGER, connected INTEGER);
            CREATE TABLE qos2_flows(client_id TEXT, packet_id INTEGER,
                state TEXT, topic TEXT, payload BLOB, updated_at REAL,
                PRIMARY KEY(client_id, packet_id));
            CREATE TABLE inbox(id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT, topic TEXT, payload BLOB, packet_id INTEGER,
                delivered_at REAL);
            """
        )
        conn.commit()
        conn.close()
        migrated = RetainedStorage(self.path)
        ep, _ = migrated.begin_connect("dev", clean=False)
        migrated.save_pending("dev", ep, 1, "rt", b"v", False, retain=True)
        migrated.finish_on_pubrel("dev", ep, 1)
        self.assertEqual(
            bytes(
                migrated.conn.execute(
                    "SELECT payload FROM retained WHERE topic='rt'"
                ).fetchone()[0]
            ),
            b"v",
        )
        migrated.close()


# ---------------------------------------------------------------------------
# 真实 TCP（--retained 子进程）
# ---------------------------------------------------------------------------


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

    def restart(self, crash: str | None = None, retained: bool = True) -> None:
        self.server.terminate()
        for p in self.peers:
            p.close()
        self.peers.clear()
        self.server = ServerProcess(
            self.db_path,
            crash=crash if crash is not None else self.crash,
            port=self.port,
            retained=retained,
        )
        self.server.start()

    def retained(self) -> dict[str, tuple[bytes, int]]:
        return retained_map(self.db_path)

    def inbox_payloads(self, client="dev") -> list[bytes]:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT payload FROM inbox WHERE client_id=? ORDER BY id", (client,)
            ).fetchall()
        finally:
            conn.close()
        return [bytes(r[0]) for r in rows]


class RetainedVisibilityTcpTests(RetainedTcpTestBase):
    def test_retained_appears_only_after_pubrel(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("rt/new", b"v1", 1, retain=True)
        p.expect_pubrec(1)
        # 等待 PUBREL 确认期间：查询不得提前显示新值
        self.assertEqual(query_retained(self.db_path, "#"), [])
        p.pubrel(1)
        p.expect_pubcomp(1)
        rows = query_retained(self.db_path, "#")
        self.assertEqual([r["topic"] for r in rows], ["rt/new"])
        self.assertEqual(bytes.fromhex(rows[0]["payload_hex"]), b"v1")
        self.assertEqual(self.inbox_payloads(), [b"v1"])

    def test_overwrite_and_delete_not_visible_while_pending(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)

        # 覆盖：PUBREC 后、PUBREL 前必须仍是旧值
        p.publish("rt", b"new", 2, retain=True)
        p.expect_pubrec(2)
        self.assertEqual(self.retained()["rt"][0], b"old")
        p.pubrel(2)
        p.expect_pubcomp(2)
        self.assertEqual(self.retained()["rt"][0], b"new")

        # 删除：pending 期间旧值仍在
        p.publish("rt", b"", 3, retain=True)
        p.expect_pubrec(3)
        self.assertIn("rt", self.retained())
        p.pubrel(3)
        p.expect_pubcomp(3)
        self.assertNotIn("rt", self.retained())

    def test_zero_payload_deletes_topic(self):
        p = self.peer()
        p.connect("dev", clean=True)
        deliver(p, "rt", b"old", 1, retain=True)
        self.assertIn("rt", self.retained())
        deliver(p, "rt", b"", 2, retain=True)
        self.assertEqual(self.retained(), {})
        self.assertEqual(self.inbox_payloads(), [b"old", b""])  # 业务账仍有记录

    def test_normal_message_does_not_update_retained(self):
        p = self.peer()
        p.connect("dev", clean=True)
        deliver(p, "rt", b"keep", 1, retain=True)
        deliver(p, "rt", b"normal", 2, retain=False)  # 普通消息不覆盖
        self.assertEqual(self.retained()["rt"][0], b"keep")
        # 全新 topic 的普通消息也不建立保留值
        deliver(p, "plain/x", b"y", 3, retain=False)
        self.assertEqual(set(self.retained()), {"rt"})

    def test_duplicate_publish_different_topic_payload_retain(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("rt", b"v1", 10, retain=True)
        p.expect_pubrec(10)
        # 重传：不同 topic、载荷、RETAIN 标志
        p.publish("other", b"v2", 10, dup=True, retain=False)
        p.expect_pubrec(10)
        p.pubrel(10)
        p.expect_pubcomp(10)
        self.assertEqual(self.inbox_payloads(), [b"v1"])  # 首次受理内容
        retained = self.retained()
        self.assertEqual(set(retained), {"rt"})
        self.assertEqual(retained["rt"][0], b"v1")
        # 重放 PUBREL：不重复交付、不改变保留值
        p.pubrel(10)
        p.expect_pubcomp(10)
        self.assertEqual(self.inbox_payloads(), [b"v1"])
        self.assertEqual(self.retained()["rt"][0], b"v1")

    def test_duplicate_publish_first_non_retain_wins(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("rt", b"v", 11, retain=False)
        p.expect_pubrec(11)
        p.publish("rt", b"v", 11, dup=True, retain=True)  # 重传却带 RETAIN
        p.expect_pubrec(11)
        p.pubrel(11)
        p.expect_pubcomp(11)
        self.assertEqual(self.retained(), {})

    def test_duplicate_publish_first_empty_payload_wins(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        p.publish("rt", b"", 12, retain=True)  # 首次：零载荷（删除）
        p.expect_pubrec(12)
        p.publish("rt", b"nonempty", 12, dup=True, retain=True)  # 重传：非空
        p.expect_pubrec(12)
        p.pubrel(12)
        p.expect_pubcomp(12)
        self.assertNotIn("rt", self.retained())  # 按首次受理执行删除

    def test_packet_id_reuse_new_message_updates_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"first", 20, retain=True)
        deliver(p, "rt", b"second", 20, retain=True)  # 编号复用，新消息
        self.assertEqual(self.retained()["rt"][0], b"second")
        self.assertEqual(self.inbox_payloads(), [b"first", b"second"])
        # 旧交换的 DUP=1 重传 + PUBREL：不得回退保留值
        p.publish("rt", b"stale", 20, dup=True, retain=True)
        p.expect_pubrec(20)
        p.pubrel(20)
        p.expect_pubcomp(20)
        self.assertEqual(self.retained()["rt"][0], b"second")
        self.assertEqual(len(self.inbox_payloads()), 2)

    def test_retained_is_global_across_clients(self):
        a = self.peer()
        a.connect("alpha", clean=True)
        deliver(a, "g/x", b"from-a", 1, retain=True)
        a.disconnect()

        b = self.peer()
        b.connect("beta", clean=True)
        deliver(b, "g/x", b"from-b", 1, retain=True)
        # 保留账不随 ClientId 隔离：后者为最新值
        self.assertEqual(self.retained()["g/x"][0], b"from-b")
        # beta 清理协议会话不得删除保留账
        b.disconnect()
        self.assertEqual(self.retained()["g/x"][0], b"from-b")


class RetainedSessionLifecycleTcpTests(RetainedTcpTestBase):
    def test_clean1_wipe_of_flows_keeps_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"keep", 1, retain=True)
        # 一个停在 pending 的 RETAIN 交换
        p.publish("rt2", b"pending-val", 2, retain=True)
        p.expect_pubrec(2)
        p2 = self.peer()
        sp, code = p2.connect("dev", clean=True)
        self.assertEqual(code, 0)
        self.assertFalse(sp)
        retained = self.retained()
        self.assertEqual(retained["rt"][0], b"keep")  # 已交付保留值不删
        self.assertNotIn("rt2", retained)  # pending 从未进保留账

    def test_takeover_cannot_corrupt_retained(self):
        old = self.peer()
        sp, _ = old.connect("dev", clean=False)
        self.assertFalse(sp)
        deliver(old, "rt", b"old-val", 1, retain=True)

        new = self.peer()
        sp, _ = new.connect("dev", clean=False)
        self.assertTrue(sp)
        self.assertTrue(old.expect_closed(timeout=5))
        # 旧连接即便竞态写入也会被 epoch 拒绝
        try:
            old.publish("rt", b"from-old", 2, retain=True)
        except OSError:
            pass
        deliver(new, "rt", b"new-val", 2, retain=True)
        self.assertEqual(self.retained()["rt"][0], b"new-val")
        self.assertEqual(self.inbox_payloads(), [b"old-val", b"new-val"])

    def test_restart_preserves_retained(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt/1", b"v1", 1, retain=True)
        deliver(p, "rt/2", b"", 2, retain=True)  # 零载荷 RETAIN：交付即删除
        deliver(p, "rt/3", b"v3", 3, retain=True)
        p.disconnect()

        self.restart(crash="")
        retained = self.retained()
        self.assertEqual(retained["rt/1"][0], b"v1")
        self.assertEqual(retained["rt/3"][0], b"v3")
        self.assertNotIn("rt/2", retained)
        # 零载荷 RETAIN 仍完成 QoS2 入账（业务账只增），删除只作用于保留账
        self.assertEqual(self.inbox_payloads(), [b"v1", b"", b"v3"])

        # 重启后重放旧 PUBREL（done 墓碑跨重启）：不重复交付、不改保留值
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.pubrel(1)
        p2.expect_pubcomp(1)
        self.assertEqual(self.retained()["rt/1"][0], b"v1")
        self.assertEqual(self.inbox_payloads(), [b"v1", b"", b"v3"])

    def test_restart_without_retained_flag_keeps_table(self):
        p = self.peer()
        p.connect("dev", clean=True)
        deliver(p, "rt", b"keep", 1, retain=True)
        p.disconnect()

        # 不以 --retained 启动：RETAIN 报文被拒，但保留账原样保留
        self.restart(crash="", retained=False)
        plain = self.peer()
        plain.connect("dev", clean=True)
        plain.publish("rt", b"x", 2, retain=True)
        self.assertTrue(plain.expect_closed())
        self.assertEqual(self.retained()["rt"][0], b"keep")

        # 再以 --retained 启动：账目继续可用
        self.restart(crash="", retained=True)
        p2 = self.peer()
        p2.connect("dev", clean=True)
        deliver(p2, "rt", b"next", 3, retain=True)
        self.assertEqual(self.retained()["rt"][0], b"next")


class RetainedCrashTcpTests(RetainedTcpTestBase):
    """四个崩溃点 × 重启重放：inbox / qos2_flows / retained 必须一致。"""

    def _drain_crashed_process(self):
        self.assertTrue(self.peers[-1].expect_closed(timeout=8))
        code = self.server.proc.wait() if self.server.proc else 137
        self.assertEqual(code, 137)
        for stream in (self.server.proc.stdout, self.server.proc.stderr):
            try:
                stream.close()
            except OSError:
                pass
        self.server.proc = None

    def test_crash_before_pending_commit_replay(self):
        # 先在无崩溃进程里种一个旧保留值
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        p.disconnect()
        self.restart(crash="before_pending_commit")

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("rt2", b"new", 2, retain=True)
        self._drain_crashed_process()

        self.restart(crash="")
        # pending 事务未提交：无残留，保留账不变
        self.assertEqual(self.retained()["rt"][0], b"old")
        self.assertNotIn("rt2", self.retained())
        p3 = self.peer()
        sp, _ = p3.connect("dev", clean=False)
        self.assertTrue(sp)
        deliver(p3, "rt2", b"new", 2, retain=True)
        self.assertEqual(self.retained()["rt2"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])

    def test_crash_after_pending_commit_retained_hidden_until_delivery(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        self.restart(crash="after_pending_commit")

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("rt", b"new", 2, retain=True)  # pending 提交后、PUBREC 前被杀
        self._drain_crashed_process()

        self.restart(crash="")
        # pending 已落盘但绝不允许提前改变保留账
        self.assertEqual(self.retained()["rt"][0], b"old")
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            state = conn.execute(
                "SELECT state FROM qos2_flows WHERE client_id='dev' AND packet_id=2"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(state, "pending")

        p3 = self.peer()
        sp, _ = p3.connect("dev", clean=False)
        self.assertTrue(sp)
        p3.publish("rt", b"new", 2, dup=True, retain=True)
        p3.expect_pubrec(2)
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertEqual(self.retained()["rt"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])

    def test_crash_before_pubrel_commit_atomic_replay(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        self.restart(crash="before_pubrel_commit")

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("rt", b"new", 2, retain=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)  # 入账事务提交前被杀
        self._drain_crashed_process()

        self.restart(crash="")
        # 三态一致：未入账、交换仍 pending、保留值仍旧
        self.assertEqual(self.retained()["rt"][0], b"old")
        self.assertEqual(
            [r for r in self.inbox_payloads() if r == b"new"], []
        )
        p3 = self.peer()
        sp, _ = p3.connect("dev", clean=False)
        self.assertTrue(sp)
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertEqual(self.retained()["rt"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])
        # 再重放一次：幂等
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertEqual(self.retained()["rt"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])

    def test_crash_after_pubrel_commit_everything_visible(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        self.restart(crash="after_pubrel_commit")

        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("rt", b"new", 2, retain=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)  # 事务已提交、PUBCOMP 前被杀
        self._drain_crashed_process()

        self.restart(crash="")
        self.assertEqual(self.retained()["rt"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            state = conn.execute(
                "SELECT state FROM qos2_flows WHERE client_id='dev' AND packet_id=2"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(state, "done")

        p3 = self.peer()
        sp, _ = p3.connect("dev", clean=False)
        self.assertTrue(sp)
        p3.pubrel(2)  # PUBCOMP 丢失重放
        p3.expect_pubcomp(2)
        self.assertEqual(self.retained()["rt"][0], b"new")
        self.assertEqual(self.inbox_payloads(), [b"old", b"new"])

    def test_crash_after_pubrel_commit_delete_visible(self):
        p = self.peer()
        p.connect("dev", clean=False)
        deliver(p, "rt", b"old", 1, retain=True)
        self.restart(crash="after_pubrel_commit")

        p2 = self.peer()
        p2.connect("dev", clean=False)
        p2.publish("rt", b"", 2, retain=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)  # 删除已提交、PUBCOMP 前被杀
        self._drain_crashed_process()

        self.restart(crash="")
        self.assertNotIn("rt", self.retained())  # 删除跨重启可见
        p3 = self.peer()
        p3.connect("dev", clean=True)
        p3.pubrel(2)
        p3.expect_pubcomp(2)
        self.assertNotIn("rt", self.retained())  # 重放不会恢复


class RetainedFilterQueryTcpTests(RetainedTcpTestBase):
    """通过真实交付建立保留账，再跑查询链（函数 + CLI 子进程）。"""

    SEED = {
        "a": b"aa",
        "a/b/c": b"abc",
        "a//c": b"aec",
        "x/": b"xempty",
        "sensors/1/temp": b"1t",
        "sensors/1/hum": b"1h",
        "sensors/2/temp": b"2t",
        "$SYS/load": b"load",
        "$SYS/uptime": b"up",
        "$things/x": b"things",
    }

    def _seed(self):
        p = self.peer()
        p.connect("seeder", clean=True)
        for pid, (topic, payload) in enumerate(sorted(self.SEED.items()), start=1):
            deliver(p, topic, payload, pid, retain=True)
        p.disconnect()

    def _topics(self, flt):
        return {r["topic"] for r in query_retained(self.db_path, flt)}

    def test_query_empty_db_returns_empty(self):
        self.assertEqual(query_retained(self.db_path, "#"), [])

    def test_exact_and_wildcard_filters(self):
        self._seed()
        self.assertEqual(self._topics("sensors/1/temp"), {"sensors/1/temp"})
        self.assertEqual(
            self._topics("sensors/+/temp"),
            {"sensors/1/temp", "sensors/2/temp"},
        )
        self.assertEqual(
            self._topics("sensors/#"),
            {"sensors/1/temp", "sensors/1/hum", "sensors/2/temp"},
        )
        self.assertEqual(
            self._topics("#"),
            {
                "a",
                "a/b/c",
                "a//c",
                "x/",
                "sensors/1/temp",
                "sensors/1/hum",
                "sensors/2/temp",
            },
        )

    def test_empty_levels_match(self):
        self._seed()
        self.assertEqual(self._topics("a/+/c"), {"a/b/c", "a//c"})
        self.assertEqual(self._topics("x/+"), {"x/"})
        self.assertEqual(self._topics("x/#"), {"x/"})
        # 末尾 # 匹配零层或多层
        self.assertEqual(
            self._topics("a/#"), {"a", "a/b/c", "a//c"}
        )

    def test_system_topic_boundary(self):
        self._seed()
        # 通配首层不触及任何 $ 主题
        self.assertNotIn("$SYS/load", self._topics("+/load"))
        self.assertNotIn("$SYS/load", self._topics("#"))
        self.assertEqual(self._topics("$SYS/+"), {"$SYS/load", "$SYS/uptime"})
        self.assertEqual(
            self._topics("$SYS/#"), {"$SYS/load", "$SYS/uptime"}
        )
        self.assertEqual(self._topics("$things/#"), {"$things/x"})
        self.assertEqual(
            self._topics("$SYS/load"), {"$SYS/load"}
        )

    def test_cli_subprocess_json(self):
        self._seed()
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
                "sensors/+/temp",
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        rows = json.loads(proc.stdout)
        self.assertEqual(
            sorted(r["topic"] for r in rows),
            ["sensors/1/temp", "sensors/2/temp"],
        )
        # 非法过滤器：CLI 以参数错误退出，不给出结果
        bad = subprocess.run(
            [
                sys.executable,
                "-m",
                "mqtt_inbox.retained_query",
                "--db",
                self.db_path,
                "--filter",
                "a/#/b",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(bad.returncode, 0)

    def test_query_is_readonly(self):
        self._seed()
        before = self.retained()
        query_retained(self.db_path, "#")
        query_retained(self.db_path, "$SYS/#")
        self.assertEqual(self.retained(), before)


if __name__ == "__main__":
    unittest.main()
