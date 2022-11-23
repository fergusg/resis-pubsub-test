import json
import os
import sys
import time

from utils import rand_t, redis

# Probably user's "bank"
STREAM = os.getenv("STREAM", "mypubsub")


def producer(p: int):
    if p == 1:
        sleep_t = rand_t()
        n = 20
    else:
        sleep_t = 1
        n = 10
    for i in range(n):
        resp = redis.publish(
            channel=STREAM,
            message=json.dumps({"time": time.time()}),  # any random JSON
        )
        if p == 1:
            sleep_t = rand_t()
        print("producer: %s" % i, resp, sleep_t)
        time.sleep(sleep_t)


def simple_consumer():
    pubsub = redis.pubsub()
    pubsub.subscribe(STREAM)

    while True:
        print("subscribing")
        for message in pubsub.listen():
            if message["type"] == "message":
                m = json.loads(message["data"])
                now = time.time()
                t = m["time"]
                print(m, now - t)
        time.sleep(1)


if __name__ == "__main__":
    opt = sys.argv[1]
    if opt == "producer":
        producer(1)
    elif opt == "C":
        simple_consumer()
    else:
        print("unknown option", opt)
