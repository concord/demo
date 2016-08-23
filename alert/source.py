import sys
import time
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)
import random
import json

def time_millis():
    return int(round(time.time() * 1000))


CAR_MAKE = ["bmw", "mercedez", "toyota", "ford"]

class CarAlert():
    def __init__(self, make, engine_needs_repair):
        self.make = make
        self.engine_needs_repair = engine_needs_repair
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=2)


def sample_alert():
    """returns a alert"""
    return CarAlert(make = random.choice(CAR_MAKE),
                    engine_needs_repair = random.random())

class CarAlertSource(Computation):
    def init(self, ctx):
        self.concord_logger.info("Source initialized")
        ctx.set_timer('loop', time_millis())

    def destroy(self):
        self.concord_logger.info("Source destroyed")

    def process_timer(self, ctx, key, time):
        # stream, key, value. empty value, no need for val
        iterations = 10
        while iterations > 0:
            iterations -= 1
            x = sample_alert()
            # self.concord_logger.debug("sending alert %s" % x.to_json())
            ctx.produce_record("alerts", x.make, x.to_json())

        # emit records every 5s
        ctx.set_timer("main_loop", time_millis() + 5000)

    def process_record(self, ctx, record): pass
    def metadata(self):
        return Metadata(
            name='car-alert-source',
            istreams=[],
            ostreams=['alerts'])

serve_computation(CarAlertSource())
