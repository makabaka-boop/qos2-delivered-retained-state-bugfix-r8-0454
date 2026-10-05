"""Opt-in retained state; visibility begins at durable QoS2 delivery."""

import sqlite3
import time
from .storage import Storage, _crash_if


class RetainedStorage(Storage):
    def __init__(self, path):
        super().__init__(path)
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(qos2_flows)")}
        if "retain" not in columns:
            try:
                self.conn.execute(
                    "ALTER TABLE qos2_flows ADD COLUMN retain INTEGER NOT NULL DEFAULT 0"
                )
            except sqlite3.OperationalError:
                if "retain" not in {
                    r[1] for r in self.conn.execute("PRAGMA table_info(qos2_flows)")
                }:
                    raise
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS retained(topic TEXT PRIMARY KEY, payload BLOB NOT NULL, inbox_id INTEGER NOT NULL)"
        )

    def save_pending(
        self, client_id, epoch, packet_id, topic, payload, dup, retain=False
    ):
        action = super().save_pending(client_id, epoch, packet_id, topic, payload, dup)
        if retain:
            if not payload:
                self.conn.execute("DELETE FROM retained WHERE topic=?", (topic,))
            else:
                self.conn.execute(
                    "INSERT INTO retained(topic,payload,inbox_id) VALUES(?,?,0) ON CONFLICT(topic) DO UPDATE SET payload=excluded.payload",
                    (topic, payload),
                )
        return action

    def finish_on_pubrel(self, client_id, epoch, packet_id):
        return super().finish_on_pubrel(client_id, epoch, packet_id)


def filter_levels(topic_filter):
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
    target = topic.split("/")
    for i, level in enumerate(levels):
        if level == "#":
            return True
        if i >= len(target) or (level != "+" and level != target[i]):
            return False
    return len(levels) == len(target)


def query_retained(path, topic_filter):
    levels = filter_levels(topic_filter)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        return [
            {"topic": r[0], "payload_hex": bytes(r[1]).hex(), "inbox_id": r[2]}
            for r in connection.execute(
                "SELECT topic,payload,inbox_id FROM retained ORDER BY topic"
            )
            if matches(r[0], levels)
        ]
    finally:
        connection.close()
