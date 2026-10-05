"""可选的本地保留状态：仅在 QoS2 成功交付的持久边界上变得可见。

保留账 ``retained`` 的更新严格遵守：

- 只由**成功交付**（PUBREL 事务提交）驱动，PUBREC 前的 pending 落盘不触碰它，
  因此“还在等待后续确认”的消息绝不会提前显示新值或删除旧值；
- 内容（topic、载荷、RETAIN 标志）以**首次受理**的 PUBLISH 为准；重复 PUBLISH
  改不动它，重复 PUBREL 不重复交付也不改变保留值；
- 与业务收件账 ``inbox``、交换完成状态 ``qos2_flows.state='done'`` 在**同一个
  SQLite 事务**内提交，崩溃/重启后三账一致；
- 零载荷 RETAIN 消息交付即删除该 topic 的保留值；普通消息（RETAIN=0）交付不动
  保留账；
- 保留账与 ClientId 协议会话生命周期分离：接管、CleanSession 清理交换状态、
  进程重启都不影响它；
- ``inbox_id`` 指向产生当前保留值的那条业务账，保证可追溯。
"""

import sqlite3

from .storage import Storage


class RetainedStorage(Storage):
    def __init__(self, path):
        super().__init__(path)
        # retain 列由基类 _migrate 保证；这里只建保留账。
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS retained("
            "topic TEXT PRIMARY KEY, "
            "payload BLOB NOT NULL, "
            "inbox_id INTEGER NOT NULL)"
        )

    def _on_delivery(self, topic, payload, retain, inbox_id):
        """在 finish_on_pubrel 的写事务内更新保留账。

        - RETAIN=0（普通消息）：保留账完全不变；
        - RETAIN=1 且零载荷：删除该 topic 的保留值；
        - RETAIN=1 且非零载荷：以本次交付内容（首次受理内容）覆盖保留值。
        重复 PUBREL 走基类的 'repeat' 分支，根本不会调用本钩子。
        """
        if not retain:
            return
        if not payload:
            self.conn.execute("DELETE FROM retained WHERE topic=?", (topic,))
        else:
            self.conn.execute(
                "INSERT INTO retained(topic, payload, inbox_id) VALUES(?,?,?) "
                "ON CONFLICT(topic) DO UPDATE SET "
                "payload=excluded.payload, inbox_id=excluded.inbox_id",
                (topic, sqlite3.Binary(payload), inbox_id),
            )


# ---------------------------------------------------------------------------
# 主题过滤器（MQTT 3.1.1 §4.7）
# ---------------------------------------------------------------------------


def filter_levels(topic_filter):
    """把过滤器拆成层并校验通配符位置；非法即 ValueError。

    - 空串非法；U+0000 非法（MQTT 1.5.3）；
    - ``+`` 必须独占一整层（``a+`` / ``+b`` / ``a+b`` 均非法）；
    - ``#`` 必须独占最后一层（``a#``、非末尾的 ``#`` 均非法）；
    - 空层（``a//b``、``a/``、``/a``）是有效层，会参与匹配。
    """
    if not isinstance(topic_filter, str) or not topic_filter or "\x00" in topic_filter:
        raise ValueError("invalid topic filter")
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if "+" in level and level != "+":
            raise ValueError("+ must occupy one whole level")
        if "#" in level and (level != "#" or i != len(levels) - 1):
            raise ValueError("# must occupy the final level")
    return levels


def matches(topic, levels):
    """按 MQTT 3.1.1 §4.7.1/§4.7.2 判断 topic 是否命中过滤器。

    - ``+`` 恰好匹配一层（包括空层）；``#`` 位于末尾，匹配零层或多层；
    - 过滤器首层为通配符（``+`` 或 ``#``）时，不匹配以 ``$`` 开头的
      系统主题（§4.7.2）；其他以 ``$`` 开头的层没有此限制。
    """
    target = topic.split("/")
    if target and target[0][:1] == "$" and levels[0] in ("+", "#"):
        return False
    for i, level in enumerate(levels):
        if level == "#":
            # 末尾多层通配：此前各层已匹配，余下零层或多层全部命中。
            return True
        if i >= len(target):
            return False
        if level != "+" and level != target[i]:
            return False
    return len(levels) == len(target)


def query_retained(path, topic_filter):
    """只读查询命中过滤器的保留状态，按 topic 排序。

    以 ``mode=ro`` + ``query_only`` 打开，绝不写入。保留表尚不存在
    （数据库从未以 --retained 运行过）时返回空列表。
    """
    levels = filter_levels(topic_filter)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        try:
            rows = connection.execute(
                "SELECT topic, payload, inbox_id FROM retained ORDER BY topic"
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        return [
            {"topic": r[0], "payload_hex": bytes(r[1]).hex(), "inbox_id": r[2]}
            for r in rows
            if matches(r[0], levels)
        ]
    finally:
        connection.close()
