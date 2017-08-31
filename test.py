from __future__ import print_function
from __future__ import unicode_literals
import sys
import logging
from pprint import pprint

from kazoo.client import KazooClient

from solrzkutil.healthy import check_ephemeral_sessions_fast, check_ephemeral_dump_consistency

logging.basicConfig()
log = logging.getLogger()
kazoo_log = logging.getLogger('kazoo.client')
kazoo_log.setLevel(logging.WARN)
log.setLevel(logging.INFO)

c = KazooClient('zk01.dev.gigdev.dhiaws.com:2181,zk02.dev.gigdev.dhiaws.com:2181,zk03.dev.gigdev.dhiaws.com:2181')
print("ZK HOSTS: ", c.hosts)

def test_check_ephemeral_session():
    response = check_ephemeral_sessions_fast(c)
    pprint(response)
    if not response:
        log.info('"check_ephemeral_sessions_fast" returned success!')
        
def test_check_ephemeral_dump_consistency():
    response = check_ephemeral_dump_consistency(c)
    pprint(response)
    if not response:
        log.info('"check_ephemeral_dump_consistency" returned success!')
        
def main(argv=None):
    test_check_ephemeral_dump_consistency()
    test_check_ephemeral_session()
    
    
if __name__ == '__main__':
    sys.exit(main())