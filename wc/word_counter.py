import sys
import concord
import time
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)
def time_millis():
    return int(round(time.time() * 1000))

class WordCounter(Computation):
    def init(self, ctx):
        self.dict = {}
        ctx.set_timer('loop', time_millis() + 5000)
        self.concord_logger.info("Counter initialized")

    def process_timer(self, ctx, key, time):
        # Print words every 5 secs
        self.concord_logger.info("Words: %s", self.dict)
        ctx.set_timer('loop', time_millis() + 5000)

    def process_record(self, ctx, record):
        if self.dict.has_key(record.key): self.dict[record.key] += 1
        else: self.dict[record.key] = 1

    def metadata(self):
        return Metadata(name='word-counter', ostreams=[],
                        istreams=[('words', StreamGrouping.GROUP_BY)])

serve_computation(WordCounter())
