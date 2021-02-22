# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)


    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        # window_size_ms given by params, used as a timeblock
        key = self.key_schema.sliding_window_rate_limiter_key(name, self.window_size_ms,
                                                              self.max_hits)
        now = datetime.datetime.utcnow().timestamp() * 1000

        pipeline = self.redis.pipeline()
        member = now + random.random()

        # add sorted set in timeblock declared in miliseconds (used in key)
        pipeline.zadd(key, {member: now})
        # removes all sorted sets in score defined by acutal datetime ms - timeblock (ms)
        pipeline.zremrangebyscore(key, 0, now - self.window_size_ms)
        # get cardinality of sorted set
        pipeline.zcard(key)
        _, _, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
