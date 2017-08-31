"""
Contains logic for health checks for Zookeeper and Solr
"""
from __future__ import unicode_literals
from __future__ import print_function
import itertools
from threading import Thread
import random
from collections import defaultdict
import time

import six
from kazoo.retry import KazooRetry

from solrzkutil.parser import parse_admin_dump, parse_admin_cons
from solrzkutil.util import netcat, parse_zk_hosts, kazoo_clients_from_client, kazoo_clients_connect

from pprint import pprint 
ZNODE_PATH_SEPARATOR = '/'

def multi_admin_command(zk_client, command):
    """
    Executes an administrative command over multiple zookeeper nodes in a session-less manner 
    using threading.
    
    Using threading not only speeds up the total time taken to query the remote Zookeeper hosts, 
    it also ensure the most similar and real-time results from the servers.
    
    :param zookeepers: a zookeeper connection string, containing all nodes you wish to execute 
                       the command against
    :param command: a 
    """
    if not isinstance(zookeepers, six.string_types):
        raise ValueError('zookeepers must be a string, got: %s' % type(zookeepers))
        
    if not isinstance(command, six.binary_type):
        raise ValueError('command must be a byte string got: %s' % type(command))
        
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=True)
    admin_results = []
    
    def get_admin(zk_host, zk_port, cmd):
        result = netcat(zk_host, zk_port, cmd)
        admin_results.append(result)

    wait = []
    for host, port in zk_client.hosts:
        t = Thread(target=get_admin, args=(host, port, command))
        t.start()
        wait.append(t)
        
    # wait for all threads to finish
    for wait_thread in wait:
        wait_thread.join()
        
    return admin_results

def znode_path_join(parts):
    """
    Given a sequence of node segments construct a fully qualified path.
    
    Can join paths from sequences like::
    
        ('/path1/path2', 'path')
        ('path1', 'path2', 'path')
        ('path1', '/path2', '/path')
        
    The output for all 3 examples above will be the path::
        
        /path1/path2/path
    """
    if not len(parts):
        raise ValueError('empty path')
        
    parts = [p.strip(ZNODE_PATH_SEPARATOR) for p in parts]
    
    # add leading slash
    parts[0] = ZNODE_PATH_SEPARATOR + parts[0]
    
    return ZNODE_PATH_SEPARATOR.join(parts)
    
def znode_path_split(path):
    """
    Given an absolute znode path returns a tuple (directory, filename)
    
    Note that you should/cannot use path functions in Python to parse znode paths as they will not
    work cross-platform (Windows).
    """
    if not path.startswith(ZNODE_PATH_SEPARATOR):
        raise ValueError('A znode path must be fully qualified and start with: "%s", got: %s' % (ZNODE_PATH_SEPARATOR, path))
        
    parts = path.split(ZNODE_PATH_SEPARATOR)[1:]
    return znode_path_join(parts[:-1]), parts[-1]

def check_ephemeral_dump_consistency(zk_client):
    """
    :param zookeepers: A zookeeper connection string (should describe all ensemble members)
    """

    
    zk_hosts = zk_client.hosts
    dump_results = multi_admin_command(zk_client, b'dump')
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
    comparisons = {tuple(sorted(pair, key=str)) for pair in itertools.product(range(len(ephemerals_compare)), repeat=2) if pair[0] != pair[1]]}
    for idx1, idx2 in comparisons:
        # Set comparison to determine differences between the two hosts
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

def check_ephemeral_znode_consistency(zk_client):
    """
    For all ephemeral znodes check to ensure their directories are consistent across hosts
    """
    
def get_ephemeral_paths_children_per_host(zk_client):
    """
    Returns a dictionary mapping znode_directory to a list of lists containing children for each node
    queried.
    """
    clients = kazoo_clients_from_client(zk_client)
    # ensure all the clients are connected
    kazoo_clients_connect(clients + [zk_client])
    
    retry = KazooRetry(max_tries=max(len(clients), 2))
    ephemeral_directories = set()
    dump_output = retry(zk_client.command, b'dump')
    # parse the output from dump, and get the znodes list from each output
    ephemeral_znodes = [znodes for session, znodes in six.viewitems(parse_admin_dump(dump_output)['ephemerals'])]
    # flatten the list of lists
    ephemeral_znodes = list(itertools.chain.from_iterable(ephemeral_znodes))
    # We assume that all the znodes that are ephemeral from the 'dump' command are files.
    # We then calculate a set of all directories to examine children in.
    ephemeral_directories = {znode_path_split(znode)[0] for znode in ephemeral_znodes}
    ephemeral_children = defaultdict(list)
    
    # holds errors that occur during operations
    errors = []
    
    # holds a mapping of znode path to list of ``kazoo.interfaces.IAsyncResult`` objects
    asyncs = defaultdict(list)
    
    # asynchronously get all children of znodes
    for zpath in ephemeral_directories:
        for client in clients:
            # note that 'cb' is a kazoo.interfaces.IAsyncResult
            cb = client.get_children_async(zpath)
            asyncs[znode].append(cb)
    
    # wait for all responses to be ready or timeout or error.
    while True:
        ready = []
        for znode, cbs in six.viewitems(asyncs):
            ready.extend(map(lambda a: a.ready(), cbs))
            
        if all(ready):
            break
        
    # gather results, or errors
    for znode, cbs in six.viewitems(asyncs):
        results = []
        for async in cbs:
            if async.exception:
                ephemeral_children[znode].append(async.exception)
            else:
                children = async.get()
                # make the children fully qualified paths
                children = [znode_path_join([znode, child]) for child in children]
                ephemeral_children[znode].append(children)
    
    return ephemeral_children
    
def check_ephemeral_sessions_fast(zk_client):
    """
    Fast ephemeral session check, ensure all ephemeral paths contain valid ephemeral 
    znodes with valid sessions.  The check is performed across servers.
    
    :param zk_client: Zookeeper connection object (KazooClient instance or subclass of)
                           start() will be called internally when the connection is used.
                           The connection instance should be configured with the hosts that are
                           members of the ensemble.
    """
    children = get_ephemeral_paths_children_per_host(zk_client)
    
    clients = kazoo_clients_from_client(zk_client)
    # ensure all the clients are connected
    kazoo_clients_connect(clients + [zk_client])
    
    # Get connection/session information
    conn_results = multi_admin_command(zk_client, b'cons')
    conn_data = map(parse_admin_cons, conn_results)
    conn_data = list(itertools.chain.from_iterable(conn_data))
    # Get a set() of all valid zookeeper sessions as integers
    valid_sessions = {con.get('sid') for con in conn_data if 'sid' in con}
    
    errors = []
    asyncs = defaultdict(dict) # maps client_idx to callbacks
    

    # note that 'cb' is a kazoo.interfaces.IAsyncResult
    for znode, children_results in six.viewitems(children):
        for client_idx, children_paths in enumerate(children_results):
            if isinstance(children_paths, Exception):
                exception = children_paths
                # see if this one is an error.
                errors.append(
                    "error from host: %s getting children nodes for path: [%s], error: %s" % (
                        zk_client.hosts[client_idx],
                        znode,
                        str(exception)
                    )
                )
                continue 
            
            for child_path in children_paths:
                cb = client.get_async(child_path)
                asyncs[client_idx][child_path] = cb
        
    while True:
        ready = []
        for client_idx, path_cbs in six.viewitems(asyncs):
            cbs = path_cbs.values()
            ready.extend(map(lambda a: a.ready(), cbs))
        if all(ready):
            break
        
    # gather results, or errors
    for client_idx, path_cbs in six.viewitems(asyncs):
        results = []
        for znode, async in six.viewitems(path_cbs):
            if async.exception:
                errors.append(
                    "error from host: %s, path: %s, error: %s" % (zk_client.hosts[client_idx], znode, str(exception))
                )
            else:
                content, stats = async.get()
                ephemeral_session = getattr(stats, 'ephemeralOwner', None)
                if not ephemeral_session:
                    continue
                
                if ephemeral_session not in valid_sessions:
                    errors.append(
                        "error from host: %s, ephemeral path: %s, session-id: [%s] does not exist on any Zookeeper server" % (
                            zk_client.hosts[client_idx], 
                            znode,
                            ephemeral_session
                        )
                    )
    