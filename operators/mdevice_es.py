import sys
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)
from optparse import OptionParser
from elasticsearch import Elasticsearch
def get_opts():
    parser = OptionParser()
    parser.add_option("-i", "--elastic-ip", dest="elastic_ip",
                      default="localhost",
                      help="elastic cluster ip")
    parser.add_option("-p", "--elastic-port", dest="elastic_port",
                      default="9200",
                      help="elastic cluster port")
    (options, args) = parser.parse_args()
    return (options, args)
def get_ip_port_tuple_from_args():
    (options,args) = get_opts();
    return (options.elastic_ip, options.elastic_port)
def get_elastic_search_connection():
    (host,p) = get_ip_port_tuple_from_args()
    es = Elasticsearch([host], port=int(p))
    return es
class MDeviceIndexer(Computation):
    def init(self, ctx):
        self.es = get_elastic_search_connection()
    def destroy(self): pass
    def process_timer(self, ctx, key, time): pass
    def process_record(self, ctx, record):
        self.es.index(index="concord", doc_type="mdevice", body=record.data)
    def metadata(self):
        return Metadata(
            name='m-device-es',
            istreams=['m-devices-json'],
            ostreams=[])

serve_computation(MDeviceIndexer())
