import sys
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)
from elasticsearch import Elasticsearch
es = Elasticsearch()
class MDeviceIndexer(Computation):
    def init(self, ctx): pass
    def destroy(self): pass
    def process_timer(self, ctx, key, time): pass
    def process_record(self, ctx, record):
        es.index(index="concord", doc_type="mdevice", body=record.data)
    def metadata(self):
        return Metadata(
            name='m-device-es',
            istreams=['m-devices-json'],
            ostreams=[])
serve_computation(MDeviceIndexer())
