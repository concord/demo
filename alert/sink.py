import sys
import concord
from concord.computation import (
    Computation,
    Metadata,
    StreamGrouping,
    serve_computation
)
import json
ENGINE_NEEDS_REPAIR_THRESHOLD = float(0.5)

class CarAlertFilter(Computation):
    def init(self, ctx):
        self.concord_logger.info("Filter initialized")
    def destroy(self):
        self.concord_logger.info("Filter destroyed")
    def process_timer(self, ctx, key, time): pass
    def process_record(self, ctx, record):
        car = json.loads(record.data)
        if float(car['engine_needs_repair']) >= ENGINE_NEEDS_REPAIR_THRESHOLD:
            self.concord_logger.info("Car needs repair %s" % record.data)
    def metadata(self):
        return Metadata(
            name='car-filter',
            istreams=[('alerts', StreamGrouping.ROUND_ROBIN)],
            ostreams=[])

serve_computation(CarAlertFilter())
