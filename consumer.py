import uuid
from redis import Redis
from redis.exceptions import ResponseError as RedisResponseError


class Consumer(object):
    def __init__(
        self, redis: Redis, stream: str, group_name: str, quiet=True, create_group=True
    ):
        self.quiet = quiet
        self.redis = redis
        self.stream = stream
        self.name = str(uuid.uuid4())
        self.group_name = group_name
        self.messages: list = []
        if create_group:
            self.create_group()

        if not quiet:
            print("creating consumer", self.name)
        self.redis.xgroup_createconsumer(
            name=self.stream, groupname=group_name, consumername=self.name
        )

    def has_results(self, results: list) -> bool:
        return not (len(results) == 0 or len(results[0][1]) == 0)

    def xreadgroup(self, *args, **kwargs):
        results = self.redis.xreadgroup(*args, **kwargs)

        if self.has_results(results):
            (_stream, messages) = results[0]
            self.messages = self.messages + messages

        return results

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        if not self.quiet:
            print()
            print("deleting consumer", self.name)
        self.redis.xgroup_delconsumer(
            name=self.stream, groupname=self.group_name, consumername=self.name
        )
        ids = [id for (id, _fields) in self.messages]
        if len(ids):
            self.redis.xack(self.stream, self.group_name, *ids)

    def create_group(self):
        try:
            self.redis.xgroup_create(
                name=self.stream, groupname=self.group_name, id="$", mkstream=True
            )
        except RedisResponseError as e:
            if str(e) == "BUSYGROUP Consumer Group name already exists":
                pass
            else:
                print(e)
        except Exception as e:
            print(e)
