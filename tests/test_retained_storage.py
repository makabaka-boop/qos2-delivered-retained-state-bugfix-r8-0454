"""保留账存储层测试：只由成功交付决定、三账同事务、会话生命周期分离。"""

import os
import tempfile
import unittest

from mqtt_inbox.retained import RetainedStorage
from mqtt_inbox.storage import SessionTakenOver, Storage


class RetainedStorageTestBase(unittest.TestCase):
    storage_cls = RetainedStorage

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.db = self.storage_cls(self.path)

    def tearDown(self):
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def open(self, cid="dev", clean=False):
        epoch, _ = self.db.begin_connect(cid, clean=clean)
        return epoch

    def deliver(self, cid, epoch, pid, topic, payload, *, retain=False, dup=False):
        self.db.save_pending(cid, epoch, pid, topic, payload, dup=dup, retain=retain)
        return self.db.finish_on_pubrel(cid, epoch, pid)

    def retained(self, topic):
        return self.db.conn.execute(
            "SELECT payload, inbox_id FROM retained WHERE topic=?", (topic,)
        ).fetchone()

    def all_retained(self):
        return {
            r[0]: (bytes(r[1]), r[2])
            for r in self.db.conn.execute(
                "SELECT topic, payload, inbox_id FROM retained"
            )
        }


class RetainedDeliveryTests(RetainedStorageTestBase):

    def test_retained_visible_only_after_delivery(self):
        ep = self.open()
        # PUBREC 之前/之后、PUBREL 之前：保留账毫无变化
        self.db.save_pending("dev", ep, 1, "t/v", b"new", dup=False, retain=True)
        self.assertIsNone(self.retained("t/v"))
        self.assertEqual(self.db.flow_state("dev", 1), "pending")
        # 完成交付后才可见，且 inbox_id 指向业务账
        self.db.finish_on_pubrel("dev", ep, 1)
        row = self.retained("t/v")
        self.assertIsNotNone(row)
        self.assertEqual(bytes(row[0]), b"new")
        inbox = self.db.query_inbox("dev")[0]
        self.assertEqual(row[1], inbox.id)

    def test_normal_message_does_not_touch_retained(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "t/v", b"v1", retain=True)
        # RETAIN=0 的同主题消息：保留值保持为 v1
        self.deliver("dev", ep, 2, "t/v", b"ordinary", retain=False)
        row = self.retained("t/v")
        self.assertEqual(bytes(row[0]), b"v1")

    def test_zero_payload_retained_deletes_topic(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "t/v", b"v1", retain=True)
        self.assertIsNotNone(self.retained("t/v"))
        self.deliver("dev", ep, 2, "t/v", b"", retain=True)
        self.assertIsNone(self.retained("t/v"))
        # 删除消息本身仍进入业务账
        self.assertEqual(self.db.inbox_count("dev", 2), 1)
        self.assertEqual(self.db.query_inbox("dev")[1].payload, b"")

    def test_zero_payload_delete_when_absent_is_noop(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "ghost", b"", retain=True)
        self.assertEqual(self.all_retained(), {})

    def test_retained_replace_tracks_latest_delivery(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "t/v", b"v1", retain=True)
        self.deliver("dev", ep, 2, "t/v", b"v2", retain=True)
        row = self.retained("t/v")
        self.assertEqual(bytes(row[0]), b"v2")
        self.assertEqual(row[1], self.db.query_inbox("dev")[1].id)

    def test_multiple_topics_independent(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "a", b"1", retain=True)
        self.deliver("dev", ep, 2, "b", b"2", retain=True)
        self.assertEqual(
            {t: v[0] for t, v in self.all_retained().items()},
            {"a": b"1", "b": b"2"},
        )

    def test_retained_is_global_not_per_client(self):
        ep1 = self.open("dev-a")
        self.deliver("dev-a", ep1, 1, "shared", b"from-a", retain=True)
        ep2 = self.open("dev-b")
        row = self.db.conn.execute(
            "SELECT payload FROM retained WHERE topic='shared'"
        ).fetchone()
        self.assertEqual(bytes(row[0]), b"from-a")
        # 另一客户端以普通消息交付，不改变保留值
        self.deliver("dev-b", ep2, 1, "shared", b"from-b", retain=False)
        row = self.db.conn.execute(
            "SELECT payload FROM retained WHERE topic='shared'"
        ).fetchone()
        self.assertEqual(bytes(row[0]), b"from-a")


class FirstAcceptedContentTests(RetainedStorageTestBase):

    def test_duplicate_publish_keeps_first_retain_and_payload(self):
        ep = self.open()
        # 首次受理：RETAIN=1, b"first"
        self.db.save_pending("dev", ep, 1, "t", b"first", dup=False, retain=True)
        # 重传改了载荷、保留标志：必须全部忽略
        self.db.save_pending("dev", ep, 1, "t", b"again", dup=True, retain=False)
        self.db.finish_on_pubrel("dev", ep, 1)
        row = self.retained("t")
        self.assertEqual(bytes(row[0]), b"first")
        self.assertEqual(self.db.query_inbox("dev")[0].payload, b"first")

    def test_duplicate_publish_pending_keeps_first_topic(self):
        ep = self.open()
        self.db.save_pending("dev", ep, 1, "orig/topic", b"v", dup=False, retain=True)
        self.db.save_pending(
            "dev", ep, 1, "other/topic", b"v2", dup=True, retain=True
        )
        self.db.finish_on_pubrel("dev", ep, 1)
        self.assertIsNone(self.retained("other/topic"))
        self.assertEqual(bytes(self.retained("orig/topic")[0]), b"v")

    def test_dup_after_done_does_not_change_retained(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "t", b"v1", retain=True)
        # 已完成交换的 DUP=1 重发，即便内容/标志不同也不生效
        self.db.save_pending("dev", ep, 1, "t", b"v2", dup=True, retain=False)
        self.assertEqual(self.db.finish_on_pubrel("dev", ep, 1), "repeat")
        self.assertEqual(self.db.inbox_count("dev", 1), 1)
        self.assertEqual(bytes(self.retained("t")[0]), b"v1")

    def test_duplicate_pubrel_does_not_change_retained(self):
        ep = self.open()
        self.deliver("dev", ep, 1, "t", b"v1", retain=True)
        # 用同编号的新消息（DUP=0）顶替墓碑之前，先重放旧 PUBREL：
        # 不重复交付、不动保留值
        self.assertEqual(self.db.finish_on_pubrel("dev", ep, 1), "repeat")
        self.assertEqual(bytes(self.retained("t")[0]), b"v1")

    def test_packet_id_reuse_allows_new_retained_message(self):
        ep = self.open()
        self.deliver("dev", ep, 7, "t", b"v1", retain=True)
        # 同一 PacketId 的新消息（DUP=0）携带新保留标志：允许再次交付
        self.db.save_pending("dev", ep, 7, "t", b"", dup=False, retain=True)
        self.assertEqual(self.db.finish_on_pubrel("dev", ep, 7), "delivered")
        self.assertIsNone(self.retained("t"))
        self.assertEqual(self.db.inbox_count("dev", 7), 2)

    def test_packet_id_reuse_normal_then_retain(self):
        ep = self.open()
        # 首次普通消息，无保留值
        self.deliver("dev", ep, 7, "t", b"plain", retain=False)
        self.assertIsNone(self.retained("t"))
        # 同编号新消息这次带 RETAIN
        self.db.save_pending("dev", ep, 7, "t", b"kept", dup=False, retain=True)
        self.db.finish_on_pubrel("dev", ep, 7)
        self.assertEqual(bytes(self.retained("t")[0]), b"kept")


class RetainedSessionLifecycleTests(RetainedStorageTestBase):

    def test_clean1_connect_and_disconnect_keep_retained(self):
        ep = self.open("dev", clean=True)
        self.deliver("dev", ep, 1, "t", b"v", retain=True)
        self.db.mark_disconnected("dev", ep, clean=True)
        # 协议会话/交换状态清空，保留账与业务账都在
        self.assertIsNone(self.db.flow_state("dev", 1))
        self.assertEqual(bytes(self.retained("t")[0]), b"v")
        self.assertEqual(self.db.inbox_count("dev", 1), 1)

        # 新客户端 clean=1 连接后仍能读到保留账
        ep2, _ = self.db.begin_connect("other", clean=True)
        self.assertEqual(
            bytes(
                self.db.conn.execute(
                    "SELECT payload FROM retained WHERE topic='t'"
                ).fetchone()[0]
            ),
            b"v",
        )
        self.db.mark_disconnected("other", ep2, clean=True)

    def test_takeover_does_not_touch_retained(self):
        ep_old, _ = self.db.begin_connect("dev", clean=False)
        self.deliver("dev", ep_old, 1, "t", b"v", retain=True)
        # 同名接管：旧 epoch 作废，保留账不受影响，旧连接也无法改账
        ep_new, _ = self.db.begin_connect("dev", clean=False)
        with self.assertRaises(SessionTakenOver):
            self.db.save_pending(
                "dev", ep_old, 2, "t", b"hacked", dup=False, retain=True
            )
        self.assertEqual(bytes(self.retained("t")[0]), b"v")
        self.deliver("dev", ep_new, 2, "t2", b"w", retain=True)
        self.assertEqual(
            {t: v[0] for t, v in self.all_retained().items()},
            {"t": b"v", "t2": b"w"},
        )

    def test_retained_survives_restart(self):
        ep = self.open("dev", clean=False)
        self.deliver("dev", ep, 1, "t", b"v", retain=True)
        self.deliver("dev", ep, 2, "gone", b"", retain=True)
        self.db.mark_disconnected("dev", ep, clean=False)
        self.db.close()

        db2 = RetainedStorage(self.path)
        rows = {
            r[0]: bytes(r[1])
            for r in db2.conn.execute("SELECT topic, payload FROM retained")
        }
        self.assertEqual(rows, {"t": b"v"})
        db2.close()

    def test_pending_never_wrote_retained_after_restart(self):
        # pending 未交付就断线 + 重启：保留账不应有任何痕迹
        ep = self.open("dev", clean=False)
        self.db.save_pending("dev", ep, 1, "t", b"v", dup=False, retain=True)
        self.db.mark_disconnected("dev", ep, clean=False)
        self.db.close()
        db2 = RetainedStorage(self.path)
        self.assertEqual(
            db2.conn.execute("SELECT COUNT(*) FROM retained").fetchone()[0], 0
        )
        db2.close()

    def test_migration_adds_retain_column_to_old_db(self):
        # 先用基类（旧 schema，无 retain 列）建库交付，再用 RetainedStorage 打开
        ep = self.open  # noqa: F841 - keep reference clarity
        base = Storage(self.path)
        epoch, _ = base.begin_connect("dev", clean=False)
        base.save_pending("dev", epoch, 1, "t", b"old", dup=False)
        base.finish_on_pubrel("dev", epoch, 1)
        base.close()

        db = RetainedStorage(self.path)
        columns = {r[1] for r in db.conn.execute("PRAGMA table_info(qos2_flows)")}
        self.assertIn("retain", columns)
        # 迁移后新交付的保留消息正常工作
        epoch2, _ = db.begin_connect("dev", clean=False)
        db.save_pending("dev", epoch2, 2, "t2", b"new", dup=False, retain=True)
        db.finish_on_pubrel("dev", epoch2, 2)
        row = db.conn.execute(
            "SELECT payload FROM retained WHERE topic='t2'"
        ).fetchone()
        self.assertEqual(bytes(row[0]), b"new")
        db.close()


if __name__ == "__main__":
    unittest.main()
