"""保留状态查询：通配匹配规则（§4.7）、$ 系统主题边界、只读 CLI。"""

import os
import tempfile
import unittest

from mqtt_inbox.retained import filter_levels, matches, query_retained
from mqtt_inbox.retained import RetainedStorage


class FilterLevelTests(unittest.TestCase):

    def test_valid_filters(self):
        self.assertEqual(filter_levels("a/b/c"), ["a", "b", "c"])
        self.assertEqual(filter_levels("a/+/c"), ["a", "+", "c"])
        self.assertEqual(filter_levels("#"), ["#"])
        self.assertEqual(filter_levels("a/#"), ["a", "#"])
        # 空层是有效层
        self.assertEqual(filter_levels("a//b"), ["a", "", "b"])
        self.assertEqual(filter_levels("/a"), ["", "a"])
        self.assertEqual(filter_levels("a/"), ["a", ""])
        self.assertEqual(filter_levels("+//+"), ["+", "", "+"])

    def test_invalid_filters(self):
        for bad in ("", "a/+b", "a+/b", "a/b#", "#/a", "a/#/b", "a\x00b"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    filter_levels(bad)


class MatchesTests(unittest.TestCase):

    def assertMatch(self, topic_filter, topic, expected):
        self.assertIs(
            matches(topic, filter_levels(topic_filter)),
            expected,
            f"{topic_filter!r} vs {topic!r}",
        )

    def test_exact(self):
        self.assertMatch("a/b", "a/b", True)
        self.assertMatch("a/b", "a/c", False)
        self.assertMatch("a/b", "a/b/c", False)
        self.assertMatch("a/b", "a", False)

    def test_single_level_plus(self):
        self.assertMatch("a/+/c", "a/b/c", True)
        self.assertMatch("a/+/c", "a/x/c", True)
        # + 恰好一层：不多不少
        self.assertMatch("a/+", "a/b/c", False)
        self.assertMatch("+/b", "a/b", True)
        # + 匹配空层
        self.assertMatch("a/+/c", "a//c", True)
        self.assertMatch("a/+", "a/", True)
        self.assertMatch("+/a", "/a", True)
        self.assertMatch("+", "", True)
        self.assertMatch("+", "a/b", False)

    def test_empty_levels_are_real(self):
        self.assertMatch("a//b", "a//b", True)
        self.assertMatch("a//b", "a/x/b", False)
        self.assertMatch("a/", "a/", True)
        self.assertMatch("a/", "a/b", False)
        self.assertMatch("/a", "/a", True)

    def test_hash_matches_zero_or_more_levels(self):
        self.assertMatch("#", "a", True)
        self.assertMatch("#", "a/b/c", True)
        self.assertMatch("a/#", "a", True)          # 零层
        self.assertMatch("a/#", "a/b", True)        # 一层
        self.assertMatch("a/#", "a/b/c/d", True)    # 多层
        self.assertMatch("a/#", "b/a", False)
        self.assertMatch("a/#", "ab", False)
        self.assertMatch("a/b/#", "a/b", True)
        self.assertMatch("a/b/#", "a/b/", True)
        self.assertMatch("a/b/#", "a/b/c", True)
        self.assertMatch("a/b/#", "a/x/c", False)

    def test_dollar_system_topic_boundary(self):
        # §4.7.2：过滤器首通配符不匹配 $ 开头的系统主题
        self.assertMatch("#", "$SYS/up", False)
        self.assertMatch("+", "$SYS", False)
        self.assertMatch("+/monitor", "$SYS/monitor", False)
        # 精确过滤器可以匹配 $ 主题
        self.assertMatch("$SYS/up", "$SYS/up", True)
        # 非首层的 $ 没有特殊待遇，+ 照常匹配
        self.assertMatch("a/+", "a/$x", True)
        self.assertMatch("a/+/c", "a/$SYS/c", True)
        # 首层具体、后面带 # 也能匹配 $ 主题（首字符不是通配符）
        self.assertMatch("$SYS/#", "$SYS/a/b", True)
        # 非 $ 开头的普通主题不受影响
        self.assertMatch("#", "a/$x", True)
        self.assertMatch("#", "$", False)


class QueryRetainedTests(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        db = RetainedStorage(self.path)
        epoch, _ = db.begin_connect("dev", clean=False)
        pid = 1
        for topic, payload in [
            ("a/b", b"ab"),
            ("a/c", b"ac"),
            ("a/x/y", b"axy"),
            ("a/", b"atrail"),
            ("/lead", b"lead"),
            ("odd//mid", b"mid"),
            ("$SYS/load", b"sysload"),
            ("$SYS/net/in", b"sysnet"),
        ]:
            db.save_pending("dev", epoch, pid, topic, payload, dup=False, retain=True)
            db.finish_on_pubrel("dev", epoch, pid)
            pid += 1
        db.close()

    def _topics(self, topic_filter):
        return [r["topic"] for r in query_retained(self.path, topic_filter)]

    def test_hash_excludes_system_topics(self):
        self.assertEqual(
            self._topics("#"),
            sorted(["/lead", "a/b", "a/c", "a/x/y", "a/", "odd//mid"]),
        )

    def test_plus_at_front_excludes_system_topics(self):
        self.assertEqual(self._topics("+/load"), [])
        self.assertEqual(
            self._topics("+/b"),
            ["a/b"],
        )

    def test_system_topics_reachable_explicitly(self):
        self.assertEqual(self._topics("$SYS/#"), ["$SYS/load", "$SYS/net/in"])
        self.assertEqual(self._topics("$SYS/load"), ["$SYS/load"])

    def test_single_level_wildcard(self):
        self.assertEqual(self._topics("a/+"), ["a/", "a/b", "a/c"])

    def test_multi_level_wildcard(self):
        self.assertEqual(self._topics("a/#"), ["a/", "a/b", "a/c", "a/x/y"])

    def test_empty_level_matching(self):
        self.assertEqual(self._topics("odd//mid"), ["odd//mid"])
        self.assertEqual(self._topics("/lead"), ["/lead"])

    def test_payload_hex_and_inbox_id(self):
        rows = query_retained(self.path, "a/b")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload_hex"], b"ab".hex())
        self.assertIsInstance(rows[0]["inbox_id"], int)
        self.assertGreater(rows[0]["inbox_id"], 0)

    def test_query_without_retained_table_returns_empty(self):
        other = self.path + ".plain"
        plain = RetainedStorage.__mro__[1](other)  # 基类 Storage：无 retained 表
        plain.close()
        try:
            self.assertEqual(query_retained(other, "#"), [])
        finally:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(other + suffix)
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    unittest.main()
