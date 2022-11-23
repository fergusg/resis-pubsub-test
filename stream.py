import json
import sys
import time

from utils import rand_t, redis, config

# Probably user's "bank"
STREAM = config.get("stream", "mypubsub")


def publish(delay=1):
    n = 20
    for _ in range(n):
        t = time.time()
        num_subscribers = redis.publish(
            channel=STREAM,
            message=json.dumps({"time": t}),  # any random JSON
        )
        sleep_t = rand_t() * delay
        print("Publishing to %s" % STREAM, num_subscribers, sleep_t, t)
        time.sleep(sleep_t)


def subscribe():
    pubsub = redis.pubsub()
    pubsub.subscribe(STREAM)
    print("Subscribing to", STREAM)
    for message in pubsub.listen():
        if message["type"] == "message":
            m = json.loads(message["data"])
            now = time.time()
            t = m["time"]
            print(m, now - t)


if __name__ == "__main__":
    opt = sys.argv[1]
    if opt == "pub":
        publish(0)
    elif opt == "sub":
        while True:
            subscribe()
            time.sleep(1)
    else:
        print("unknown option", opt)
