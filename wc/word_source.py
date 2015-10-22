import sys
import time
import random
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation,
    StreamGrouping
)

def time_millis():
    return int(round(time.time() * 1000))

class WordSource(Computation):
    def init(self, ctx):
        self.log = self.concord_logger; # rsi friendly
        self.log.info("Reading database in init")
        try:
            self.db = open('words.data').read().split(',')
        except Exception as e:
            self.log.error("Could not read the database file words.data: %s", e)
            sys.exit(1)
        self.log.info("Setting loop")
        ctx.set_timer('loop', time_millis())

    def process_timer(self, ctx, key, time):
        if len(self.db) == 0:
            self.log.info("Done emitting all words - harakiri")
            sys.exit(0)
        i = 0
        while i < 100000:
            i += 1
            ctx.produce_record("words", self.db.pop(), '-')
        # emit one word at least every second - shuffle for monitor tools
        ctx.set_timer(key, time_millis() + random.randint(0,1000))

    def metadata(self):
        return Metadata(name='word-source', istreams=[], ostreams=['words'])

serve_computation(WordSource())
