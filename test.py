from __future__ import print_function
from __future__ import unicode_literals
import sys
import logging
from pprint import pprint

from kazoo.client import KazooClient

from solrzkutil.healthy import (check_ephemeral_sessions_fast, 
                                check_ephemeral_dump_consistency, 
                                check_zookeeper_connectivity, 
                                check_ephemeral_znode_consistency,
                                get_solr_session_ids)

logging.basicConfig()
log = logging.getLogger()
kazoo_log = logging.getLogger('kazoo.client')
kazoo_log.setLevel(logging.INFO)
log.setLevel(logging.DEBUG)

c = KazooClient('zk01.dev.gigdev.dhiaws.com:2181,zk02.dev.gigdev.dhiaws.com:2181,zk03.dev.gigdev.dhiaws.com:2181')
print("ZK HOSTS: ", c.hosts)

def test_check_zookeeper_connectivity():
    response = check_zookeeper_connectivity(c)
    pprint(response)
    if not response:
        log.info('"check_zookeeper_connectivity" returned success!')
        
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
       
def test_check_ephemeral_znode_consistency():
    response = check_ephemeral_znode_consistency(c)
    pprint(response)
    if not response:
        log.info('"check_ephemeral_znode_consistency" returned success!')
        
def test_get_solr_session_ids():
    response = get_solr_session_ids(c)
    pprint(response)
        
def main(argv=None):
    #test_check_zookeeper_connectivity()
    #test_check_ephemeral_dump_consistency()
    #test_check_ephemeral_session()
    #test_check_ephemeral_znode_consistency()
    test_get_solr_session_ids()
    
    
if __name__ == '__main__':
    sys.exit(main())