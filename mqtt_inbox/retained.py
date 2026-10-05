"""可选保留状态（--retained）：仅在 QoS2 成功交付的持久边界上可见。

保留账（``retained`` 表）的更新规则（MQTT 3.1.1 §3.3.1.3 语义）：

- 只有 **RETAIN 标志为 1 且交换成功交付**（PUBREL 与业务账同一事务提交）
  的消息才能改变保留账；
- RETAIN 消息载荷为空 -> **删除**该 topic 的保留值；
- RETAIN 消息载荷非空 -> 按 topic upsert 为最新值（记录产生它的 inbox 行）；
- 普通消息（RETAIN=0）不触碰保留账；
- 重复 PUBLISH/PUBREL 不重复交付，也不改变保留值——保留值由**首次受理**
  的 topic/载荷/RETAIN 决定；
- 保留账独立于 ClientId 协议会话：接管、CleanSession=1 清理交换状态、
  服务重启都不删除已有保留值。

保留查询过滤器（§4.7）：``+`` 恰好匹配一个层（空层也是有效层），
``#`` 只能位于末尾并匹配零个或多个层；过滤器首层为通配层时不匹配
``$`` 开头的系统主题（§4.7.2）。
"""

from __future__ import annotations

import sqlite3

from .storage import Storage

#: 系统主题前缀：首层为通配符（+/ #）的过滤器不匹配 $ 主题（MQTT 4.7.2）。
SYSTEM_PREFIX = "$"

SCHEMA_RETAINED = """
CREATE TABLE IF NOT EXISTS retained (
    topic    TEXT PRIMARY KEY,
    payload  BLOB NOT NULL,
    inbox_id INTEGER NOT NULL
);
"""


class RetainedStorage(Storage):
    """启用本地保留状态的存储：基类负责全部 QoS2/会话事务，
    本类只在交付事务内挂上保留账影响，并保证该表随库存在。"""

    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(SCHEMA_RETAINED)

    @staticmethod
    def _apply_retained_effect(conn, topic: str, payload: bytes, inbox_id: int) -> None:
        """在 finish_on_pubrel 的入账事务内执行：零载荷删除，否则 upsert。

        与 inbox 插入、qos2_flows 置 done 同一事务提交，因此持久边界上
        业务账、交换完成状态、保留账三者恒一致。
        """
        if not payload:
            conn.execute("DELETE FROM retained WHERE topic=?", (topic,))
        else:
            conn.execute(
                "INSERT INTO retained(topic, payload, inbox_id) VALUES(?,?,?) "
                "ON CONFLICT(topic) DO UPDATE SET "
                "payload=excluded.payload, inbox_id=excluded.inbox_id",
                (topic, sqlite3.Binary(payload), inbox_id),
            )


# ---------------------------------------------------------------------------
# 主题过滤器
# ---------------------------------------------------------------------------


def filter_levels(topic_filter: str) -> list[str]:
    """校验并切分主题过滤器。

    - 过滤器必须是非空字符串且不含 U+0000；
    - ``+`` 必须独占一层（``a+b`` 非法）；
    - ``#`` 必须独占**最后一层**（``a/#/b``、``a#`` 非法）；
    - 空层是有效层：``a//b``、``a/``、``/a`` 均合法。
    """
    if not isinstance(topic_filter, str) or not topic_filter or "\x00" in topic_filter:
        raise ValueError("invalid topic filter")
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if "+" in level and level != "+":
            raise ValueError("'+' must occupy one whole level")
        if "#" in level and (level != "#" or i != len(levels) - 1):
            raise ValueError("'#' must occupy the final level")
    return levels


def matches(topic: str, levels: list[str]) -> bool:
    """判定具体主题是否匹配已校验的过滤器层序列。

    - ``+`` 恰好匹配一层（含空层）；
    - 末尾 ``#`` 匹配零层或多层；
    - 首层为通配层（``+``/``#``）的过滤器不匹配 ``$`` 开头的系统主题；
      过滤器显式以 ``$`` 开头时按普通规则匹配。
    """
    target = topic.split("/")

    # §4.7.2：服务器不应把 $ 系统主题匹配到首层为通配符的过滤器。
    first = levels[0]
    if first in ("+", "#") and topic.startswith(SYSTEM_PREFIX):
        return False

    for i, level in enumerate(levels):
        if level == "#":
            # 末尾多层通配：到这里层前缀已相等，剩余任意层（含零层）均匹配。
            return True
        if i >= len(target):
            return False
        if level != "+" and level != target[i]:
            return False
    return len(levels) == len(target)


def query_retained(path: str, topic_filter: str) -> list[dict]:
    """只读查询保留账（SQLite mode=ro，绝不写入），按过滤器返回。

    保留表尚不存在（从未成功交付过 RETAIN 消息，或旧库未启用功能）时
    返回空列表，而不是因只读连接无法建表而报错。
    """
    levels = filter_levels(topic_filter)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='retained'"
        ).fetchone()
        if table_exists is None:
            return []
        return [
            {
                "topic": r[0],
                "payload_hex": bytes(r[1]).hex(),
                "inbox_id": r[2],
            }
            for r in connection.execute(
                "SELECT topic, payload, inbox_id FROM retained ORDER BY topic"
            )
            if matches(r[0], levels)
        ]
    finally:
        connection.close()
