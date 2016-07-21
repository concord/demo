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
    if host == None or p == None:
        raise Exception("Could not get ip:port for elastic search")

    print "Elasticsearch: host ", host, " and port ", p
    es = Elasticsearch(
        [host],
        port=int(p),
        # sniff before doing anything
        sniff_on_start=True,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60
    )
    return es

class MDeviceIndexer(Computation):
    def init(self, ctx):
        self.es = get_elastic_search_connection()
        # ignore 400 cause by IndexAlreadyExistsException when creating an index
        self.es.indices.create(index='concord', ignore=400)
    def destroy(self): pass
    def process_timer(self, ctx, key, time): pass
    def process_record(self, ctx, record):
        try:
            res = self.es.index(index="concord",
                                doc_type="mdevice",
                                id=record.key,
                                body=record.data)
            if not res['created']:
                print "Error saving to elastic search: ", res
        except Exception as e:
            print "Couldn't index record: ", e
    def metadata(self):
        return Metadata(
            name='m-device-es',
            istreams=['m-devices-json'],
            ostreams=[])

serve_computation(MDeviceIndexer())
