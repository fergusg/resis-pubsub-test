import time
from random import random
from typing import Tuple
from dotenv import dotenv_values
from redis import Redis

config = dotenv_values(".env")


def btos(s: bytes) -> str:
    return s.decode("utf-8")


def parse_id(id: bytes) -> Tuple[int, int]:
    s = id.decode("utf-8")
    (ts, seq) = s.split("-")
    return (int(ts), int(seq))


def gen_group_name(group_id: str) -> str:
    return f"group-{group_id}"


def rand_t():
    return random() * 0.1


def dot():
    print(".", end="", flush=True)


def has_results(results: list) -> bool:
    return not (len(results) == 0 or len(results[0][1]) == 0)


def ack_all(messages: list, group_name: str, stream=None):
    ids = [id for (id, _fields) in messages]
    if len(ids):
        redis.xack(stream, group_name, *ids)


def purge(delete=True, delta=600_000, quiet=False, stream=None):
    """
    Delete idle consumers and groups with no consumers.

    This func should be a dud as all consumers are ephemeral and should be deleted when they exit.
    """
    for g in redis.xinfo_groups(stream):
        if g["consumers"] == 0:
            continue
        else:
            print("warning: group has consumers")
            print(g)
        grp_name = g["name"]
        for c in redis.xinfo_consumers(stream, grp_name):
            if not quiet:
                print("\t", c)
            # Hmmm idle doesn't seem to reset as advertised.
            if delete and c["idle"] > delta:
                print("deleting consumer", c["name"])
                redis.xgroup_delconsumer(stream, grp_name, c["name"])


redis = Redis(
    host=config.get("host", "localhost"),
    port=int(config.get("port", 6379)),
    password=config.get("password", None),
    db=int(config.get("db", 0)),
)


def filter_old_message(skip_t: int = 10_000) -> bool:
    def filter_message(message: Tuple[str, any], skip_t: int) -> bool:
        (id, _fields) = message
        (ts, _seq) = parse_id(id)

        now = int(time.time() * 1000)
        # Ignore messages older than skip_t ms
        return now - ts < skip_t

    return lambda message: filter_message(message, skip_t)
