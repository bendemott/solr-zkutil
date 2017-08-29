"""
Contains logic for health checks for Zookeeper and Solr
"""
from __future__ import unicode_literals
from __future__ import print_function
import itertools
from threading import Thread

import six

from solrzkutil.parser import parse_admin_dump
from solrzkutil.util import netcat, parse_zk_hosts

from pprint import pprint 

import random # XXX

def multi_admin_command(zookeepers, command):
    if not isinstance(zookeepers, six.string_types):
        raise ValueError('zookeepers must be a string, got: %s' % type(zookeepers))
        
    if not isinstance(command, six.string_types):
        raise ValueError('command must be a byte string got: %s' % type(command))
        
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=True)
    admin_results = []
    
    def get_admin(zk_host, cmd):
        hostaddr, port = zk_host.split(':')
        result = netcat(hostaddr, port, cmd)
        admin_results.append(result)

    wait = []
    for host in zk_hosts:
        t = Thread(target=get_admin, args=(host, command))
        t.start()
        wait.append(t)
        
    # wait for all threads to finish
    for wait_thread in wait:
        wait_thread.join()
        
    return admin_results
    

def check_ephemeral_consistency(zookeepers):
    """
    :param zookeepers: A zookeeper connection string (should describe all ensemble members)
    """

    
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=True)
    dump_results = multi_admin_command(zookeepers, b'dump')
    ephemerals = [parse_admin_dump(item)['ephemerals'] for item in dump_results]
    
    # Flatten the data structure returned by parsing the 'dump' command so that we have
    # a sequence (list) of sets that can be compared using set operations.
    ephemerals_compare = []
    for ephemerals in ephemerals:
        ephemeral_set = set()
        for session, paths in six.viewitems(ephemerals):
            for path in paths:
                ephemeral_set.add((session, path))
                
        ephemerals_compare.append(ephemeral_set)
        
    # Find all unique sets of indexes to use for comparisons.
    errors = []
    comparisons = set([tuple(sorted(pair, key=str)) for pair in itertools.product(range(len(ephemerals_compare)), repeat=2) if pair[0] != pair[1]])
    for idx1, idx2 in comparisons:
        differences = ephemerals_compare[idx1] ^ ephemerals_compare[idx2]
        if differences:
            errors.append(
                'ephemeral nodes do not match for host:{host1} and host:{host2}... differences: {diff}'.format(
                    host1=zk_hosts[idx1],
                    host2=zk_hosts[idx2],
                    diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in differences])
                ) 
            )
    return errors

def check_ephemeral_sessions_fast(zk):
    """
    Fast ephemeral session check, ensure all ephemeral paths contain valid ephemeral 
    znodes with sessions.
    """
    # calling 'dump', parse directories 
    # gather all sessions from 'cons' command and parse the results.
    
    