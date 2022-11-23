from random import random
from dotenv import dotenv_values
from redis import Redis

config = dotenv_values(".env")


def btos(s: bytes) -> str:
    return s.decode("utf-8")


def rand_t():
    return random() * 0.1


redis = Redis(
    host=config.get("host", "localhost"),
    port=int(config.get("port", 6379)),
    password=config.get("password", None),
    db=int(config.get("db", 0)),
)
