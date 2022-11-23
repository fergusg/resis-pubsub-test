import os
import sys
import time
import redis as R
from consumer import Consumer
from utils import (
    dot,
    filter_old_message,
    gen_group_name,
    has_results,
    purge,
    rand_t,
    redis,
    btos,
)

# Probably user's "bank"
STREAM = os.getenv("STREAM", "mystream")


def reset_stream():
    redis.xtrim(name=STREAM, maxlen=0, approximate=False)


def default_on_message(*args, **kwargs):
    # do something with the message here
    def default(*args):
        (id, msg) = args
        print(f"Got message", btos(id), msg)

    return default(*args)


def default_filter(*args, **kwargs) -> bool:
    return True


def handle_messages(
    messages: list,
    group_name: str,
    last_id="0-0",
    filter_fn=None,
    on_message=None,
) -> str:
    if filter_fn is None:
        filter_fn = default_filter
    if on_message is None:
        on_message = default_on_message
    ids = []
    try:
        for message in messages:
            (id, fields) = message
            try:
                if filter_fn(message):
                    on_message(id, fields)
                else:
                    print(f"Skipping message: {id}")
            except Exception as e:
                print(e)
            finally:
                last_id = id
                ids.append(id)
    except Exception as e:
        print(e)
    finally:
        redis.xack(STREAM, group_name, *ids)
        return last_id


# needed?
def create_group(group_name: str):
    try:
        redis.xgroup_create(name=STREAM, groupname=group_name, id="$", mkstream=True)
    except R.exceptions.ResponseError as e:
        if str(e) == "BUSYGROUP Consumer Group name already exists":
            return
        raise


def producer(p: int):
    if p == 1:
        sleep_t = rand_t()
        n = 20
    else:
        sleep_t = 1
        n = 10
    for i in range(n):
        resp = redis.xadd(
            name=STREAM,
            fields={"seq_id": i, "producer": p},  # any random JSON
            id="*",  # auto-generate id
            maxlen=100,  # keep last 100 messages (approx, but always at least 100)
            approximate=True,  # don't worry about exact maxlen - much faster this way
        )
        if p == 1:
            sleep_t = rand_t()
        print("producer: %s" % i, resp, sleep_t)
        time.sleep(sleep_t)


def simple_consumer(group_name: str, last_id: str = ">", quiet=True):
    with Consumer(
        group_name=group_name, redis=redis, stream=STREAM, quiet=quiet
    ) as consumer:
        results = consumer.xreadgroup(
            groupname=group_name,
            consumername=consumer.name,
            streams={STREAM: last_id},
            count=None,  # get all messages
            block=500,
            noack=False,
        )

        if not has_results(results):
            dot()
            return
        print()
        # only 1 stream
        (_stream, messages) = results[0]

        handle_messages(
            messages=messages,
            group_name=group_name,
            filter_fn=filter_old_message(),
            on_message=default_on_message,
        )


if __name__ == "__main__":
    opt = sys.argv[1]
    if opt == "producer1":
        producer(1)
    elif opt == "producer2":
        producer(2)
    elif opt == "C":
        group_name = gen_group_name(opt)
        simple_consumer(group_name, quiet=False)
    elif opt == "D" or opt == "E":
        group_name = gen_group_name(opt)
        while True:
            simple_consumer(group_name, quiet=True)
    elif opt == "consumers":
        purge(delete=False, stream=STREAM)
    elif opt == "purge":
        purge(stream=STREAM)
        # Need to call twice to delete groups with no consumers
        purge(quiet=True, stream=STREAM)
    elif opt == "purge-all":
        purge(delta=0, stream=STREAM)
        purge(delta=0, quiet=True, stream=STREAM)
    elif opt == "reset":
        reset_stream()
    else:
        print("unknown option", opt)
