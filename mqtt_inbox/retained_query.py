import argparse
import json
from .retained import query_retained


def main():
    p = argparse.ArgumentParser(description="Read delivered retained topic state")
    p.add_argument("--db", required=True)
    p.add_argument("--filter", required=True)
    a = p.parse_args()
    try:
        print(json.dumps(query_retained(a.db, a.filter), ensure_ascii=False))
    except ValueError as exc:
        p.error(str(exc))


if __name__ == "__main__":
    main()
