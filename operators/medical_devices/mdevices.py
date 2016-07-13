import sys
import time
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)
# google's gumbo only works w/ BeautifulSoup3
import BeautifulSoup
import requests
import gumbo
import re

import hashlib

def time_millis():
    return int(round(time.time() * 1000))

# returns byte array
def url_hash(x):
    s = str(x)
    m = hashlib.md5()
    m.update(s)
    return m.digest()

def raw_urls():
    def link_extractor(attr_array):
        for t in attr_array:
            if len(t) == 2:
                (href, link) = t
                if href == "href" and len(link) > 0:
                    return link
        return None

    urls = []
    try:
        req = requests.get("http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/DeviceApprovalsandClearances/510kClearances/ucm089428.htm")
        soup = gumbo.soup_parse(req.text)
        links = soup.findAll('a', href=re.compile('.*\.zip'))
        attrs = map(lambda x: x.attrs, links)
        urls = map(link_extractor, attrs)
    except:
        urls = []
    return urls

class MedicalDevicesUrlGenerator(Computation):
    def init(self, ctx):
        self.concord_logger.info("MedicalDevicesUrlGenerator init")
        ctx.set_timer('loop', time_millis())
    def destroy(self):
        self.concord_logger.info("MedicalDevicesUrlGenerator destroyed")
    def process_timer(self, ctx, key, time):
        urls = raw_urls()
        for url in urls:
            # check in the cache if we have already processed this url
            h = url_hash(url)
            if len(ctx.get_state(h)) == 0:
                url_b = bytes(url)
                ctx.set_state(h, url_b)
                ctx.produce_record("m-device-urls", h, url_b)

        delay_ms = 1000 * 60 * 10; # 10 minutes
        ctx.set_timer(key, time_millis() + delay_ms)
    def process_record(self, ctx, record):
        raise Exception('process_record not implemented')
    def metadata(self):
        return Metadata(
            name='mdevices',
            istreams=[],
            ostreams=['m-device-urls'])

serve_computation(MedicalDevicesUrlGenerator())
