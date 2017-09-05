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
from pprint import pformat
import logging
log = logging.getLogger(__name__)

import six
from kazoo.retry import KazooRetry
from kazoo.client import KazooClient

from solrzkutil.parser import parse_admin_dump, parse_admin_cons, parse_admin_wchp
from solrzkutil.util import netcat, parse_zk_hosts, kazoo_clients_from_client, kazoo_clients_connect, kazoo_client_cache_enable

from pprint import pprint 
ZNODE_PATH_SEPARATOR = '/'

def multi_admin_command(zk_client, command):
    """
    Executes an administrative command over multiple zookeeper nodes in a session-less manner 
    using threading.
    
    Using threading not only speeds up the total time taken to query the remote Zookeeper hosts, 
    it also ensures the most similar and real-time results from the servers.
    
    :param zookeepers: a zookeeper connection string, containing all nodes you wish to execute 
                       the command against
    :param command: a 
    """
    if not isinstance(command, six.binary_type):
        raise ValueError('command must be a byte string got: %s' % type(command))
        
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
    
        ('/', '/path1/path2', 'path')
        ('path1', 'path2', 'path')
        ('path1', '/path2', '/path')
        
    The output for all 3 examples above will be the path::
        
        /path1/path2/path
    """
    if not len(parts):
        raise ValueError('empty path %s' % parts)
        
    parts = [p.strip(ZNODE_PATH_SEPARATOR) for p in parts if p.strip(ZNODE_PATH_SEPARATOR).strip()]
    
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
    Check the consistency of 'dump' output across Zookeeper hosts
    
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
    comparisons = {tuple(sorted(pair, key=str)) for pair in itertools.product(range(len(ephemerals_compare)), repeat=2) if pair[0] != pair[1]}
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
            
    if not errors:
        log.debug('%s.%s encountered no errors' % (__name__, check_ephemeral_dump_consistency.__name__))
            
    return errors

def check_session_file_watches(zookeepers):

    def check_file_watches(zk_client):
        wchp_results = multi_admin_command(zk_client, b'wchp')
        wchp_clusterprops_wchs = [parse_admin_wchp(item)[u'/clusterprops.json'] for item in wchp_results]
        wchp_clusterstate_wchs = [parse_admin_wchp(item)[u'/clusterstate.json'] for item in wchp_results]
        wchp_aliases_wchs = [parse_admin_wchp(item)[u'/aliases.json'] for item in wchp_results]

        file_watches_compare = []
        # TODO: Remove Test Code
        # x = list(set(wchp_clusterprops_wchs[0]))
        # x.append(1234)
        # file_watches_compare.append(["clusterprops.json", set(x)])

        file_watches_compare.append(["clusterprops.json", set(list(set(wchp_clusterprops_wchs[0])))])
        file_watches_compare.append(["clusterstate.json", set(list(set(wchp_clusterstate_wchs[0])))])
        file_watches_compare.append(["aliases.json", set(list(set(wchp_aliases_wchs[0])))])

        # log.info("file watches: %s" % (file_watches_compare))

        comparisons = {tuple(sorted(pair, key=str)) for pair in itertools.product(range(len(file_watches_compare)), repeat=2) if pair[0] != pair[1]}

        errors = []
        for idx1, idx2 in comparisons:
            differences = file_watches_compare[idx1][1] ^ file_watches_compare[idx2][1]
            if differences:
                errors.append(
                    '{zk_host} sessions watches do not match files:{file1} and {file2}... differences: {diff}'.format(
                        zk_host = zk_client.hosts,
                        file1=file_watches_compare[idx1][0],
                        file2=file_watches_compare[idx2][0],
                        diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in differences])
                    )
                )

        return errors

    all_errors = []

    for zk in zookeepers.split(','):
        zk_client = KazooClient(zk)
        all_errors.append(check_file_watches(zk_client))

    return all_errors

def check_zookeeper_connectivity(zk_client, min_timeout=2):
    """
    Check zookeeper connectivity responsiveness
    """
    kazoo_client_cache_enable(False)
    clients = kazoo_clients_from_client(zk_client)
    # ensure all the clients are connected
    errors = []
    for timeout in range(min_timeout, 10):
        connected = kazoo_clients_connect(clients, continue_on_error=True)
        if connected < len(clients):
            errors.append('%d clients unable to connect within %d secs' % (
                len(clients) - connected, timeout))
        else:
            break
            
    kazoo_client_cache_enable(True)
    return errors
    
def get_async_ready(asyncs):
    """
    Given a dictionary containing async objects wait for them all to become ready before returning.
    
    :param asyscs: asynchronous requests that have been started.
    
    Asyncs structure is::
    
        {
            arg_0: {
                0: kazoo.interfaces.IAsyncResult
                1: kazoo.interfaces.IAsyncResult
                2: kazoo.interfaces.IAsyncResult
            },
            arg_1: {
                0: kazoo.interfaces.IAsyncResult
                1: kazoo.interfaces.IAsyncResult
                2: kazoo.interfaces.IAsyncResult
            },
        }
    """
    while True:
        ready = []
        for asyncs_per_host in asyncs.values():
            async_results = asyncs_per_host.values()
            ready.extend(map(lambda a: a.ready(), async_results))
            
        if all(ready):
            break
            
    return True
    
def get_async_call_per_host(zk_client, args, call):
    """
    :param args: arguments to pass into ``call``, this should be a list of znode paths for example.
    :param call: a callable that accepts two arguments (KazooClient, arg)
                 where arg is an entry from args
                 
    ``call`` should usually be a lambda such as::
    
        lambda c, arg: c.get(arg)
                 
    returns a dictionary like::
    
        {
            arg_0: {
                0: result or exception obj
                1: result or exception obj 
                2: result or exception obj 
            },
            arg_1: {
                0: result or exception obj
                1: result or exception obj 
                2: result or exception obj 
            },
        }
    """
    clients = kazoo_clients_from_client(zk_client)
    kazoo_clients_connect(clients)
    
    asyncs = defaultdict(dict)
    for arg in args:
        for client_idx, client in enumerate(clients):
            asyncs[arg][client_idx] = call(client, arg)
    
    # block until the calls complete
    get_async_ready(asyncs)
    
    results = defaultdict(dict)
    for arg, host_async in six.viewitems(asyncs):
        for host_idx, async_result in six.viewitems(host_async):
            results[arg][host_idx] = async_result.exception or async_result.get()
            
    return results
            
    
def get_ephemeral_paths_children_per_host(zk_client):
    """
    
    Returns a dictionary mapping znode_directory to a list of lists containing children for each node
    queried.
    
    returns a dictionary like::
    
        {
            arg_0: {
                0: result or exception obj
                1: result or exception obj 
                2: result or exception obj 
            },
            arg_1: {
                0: result or exception obj
                1: result or exception obj 
                2: result or exception obj 
            },
        }
    """
    # get 1 KazooClient per Zookeeper host.
    clients = kazoo_clients_from_client(zk_client)
    # ensure all the clients are connected
    kazoo_clients_connect(clients)
    
    retry = KazooRetry(max_tries=max(len(clients), 2))
    ephemeral_directories = set()
    #dump_output = retry(zk_client.command, b'dump')
    dump_results = multi_admin_command(zk_client, b'dump')
    
    
    # We'll combine all of the results from each available servers dump result.
    # the results from each server should be identical, but just in case we'll combine all results.
    ephemeral_znodes = []
    for host_result in dump_results:
        # parse the output from dump, and get the znodes list from each output
        ephemeral_znodes.extend([znodes for session, znodes in six.viewitems(parse_admin_dump(host_result)['ephemerals'])])
    # flatten the list of lists
    ephemeral_znodes = sorted(set(itertools.chain.from_iterable(ephemeral_znodes)))
    log.debug("ephemeral paths resolved: %d, ...\n%s" % (len(ephemeral_znodes), pformat(ephemeral_znodes)))
    # We assume that all the znodes that are ephemeral from the 'dump' command are files.
    # We then calculate a set of all directories to examine children in.
    ephemeral_directories = [] 
    for znode in ephemeral_znodes:
        if not znode or znode.strip() == ZNODE_PATH_SEPARATOR:
            log.warn('a znode returned from `dump` is unexpectedly empty: "%s", the output of dump is: %s' % (znode, dump_output))
        try:
            ephemeral_directories.append(znode_path_split(znode)[0])
        except Exception as e:
            log.error('exception while getting znode path: "%s", the output of dump is: %s' % (znode, dump_output))
            continue
            
    ephemeral_directories = sorted(set(ephemeral_directories))
    
    def call(client, znode):
        return client.get_children_async(znode)
        
    results = get_async_call_per_host(zk_client, ephemeral_directories, call)
    
    return results
    
def get_async_call_per_host_errors(zk_client, async_result):
    """
    Return a list of errors contained within the response of ephemeral_children
    """
    hosts = zk_client.hosts
    # note that 'cb' is a kazoo.interfaces.IAsyncResult
    errors = []
    for arg, host_result in six.viewitems(async_result):
        for client_idx, result in six.viewitems(host_result):
            if isinstance(result, Exception):
                exception = result
                # see if this one is an error.
                errors.append(
                    "error from host: %s for input: [%s], error: %s" % (
                        hosts[client_idx],
                        arg,
                        str(exception)
                    )
                )
                continue 
                
    return errors
    
def get_ephemeral_paths_children_per_host_paths(ephemeral_children):
    """
    Get a simple list of unique paths returned by ``get_ephemeral_paths_children_per_host``
    """
    paths = set()

    for parent_path, host_children in six.viewitems(ephemeral_children):
        for client_idx, child_paths in six.viewitems(host_children):
            if isinstance(child_paths, Exception):
                continue
            for child_path in child_paths:
                paths.add(znode_path_join([parent_path, child_path]))
    
    return sorted(paths)
    
def check_ephemeral_znode_consistency(zk_client):
    """
    For all ephemeral znodes check to ensure their directories are consistent across hosts,
    as well as the content of each node, and their ephemral session / owner.
    """
    # Connect to each Zookeeper Host
    zk_hosts = zk_client.hosts
    clients = kazoo_clients_from_client(zk_client)
    kazoo_clients_connect(clients)
    
    children_results = get_ephemeral_paths_children_per_host(zk_client)
    errors = get_async_call_per_host_errors(zk_client, children_results)
    child_paths = get_ephemeral_paths_children_per_host_paths(children_results)
    
    # Check the children are consistent across hosts for each node queried.
    log.debug('checking %d paths that contain ephemerals for consistent children' % len(set(children_results.keys())))
    for parent_path, host_children in six.viewitems(children_results):
        children_sets = [frozenset(host_children[idx]) for idx in range(len(host_children)) if not isinstance(host_children[idx], Exception)]
        comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(children_sets)), repeat=2) if pair[0] != pair[1]}
        for idx1, idx2 in comparisons:
            # Set comparison to determine differences between the two hosts
            differences = children_sets[idx1] ^ children_sets[idx2]
            if not differences:
                continue
                
            errors.append(
                'ephemeral path [{parent}] contains inconsistent child nodes for host:{host1} and host:{host2}... differences: {diff}'.format(
                    parent=parent_path,
                    host1=zk_hosts[idx1],
                    host2=zk_hosts[idx2],
                    diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in differences])
                )
            )
    
    def call(client, znode):
        return client.get_async(znode)
        
    log.debug('checking %d paths for consistent content / stats' % len(child_paths))
    znode_results = get_async_call_per_host(zk_client, child_paths, call)
    errors.extend(get_async_call_per_host_errors(zk_client, znode_results))
    
    for path, host_results in six.viewitems(znode_results):
        znode_content_sets = [ frozenset(['line %d: %s' % (lidx, line) for lidx, line in enumerate((host_results[idx][0] or '').splitlines())])
                            for idx in range(len(host_results))
                            if not isinstance(host_results[idx], Exception)]
        comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(znode_content_sets)), repeat=2) if pair[0] != pair[1]}
        for idx1, idx2 in comparisons:
            # Set comparison to determine differences between the two hosts
            differences = znode_content_sets[idx1] ^ znode_content_sets[idx2]
            if not differences:
                continue
                
            errors.append(
                'ephemeral path [{path}] contains inconsistent content for host:{host1} and host:{host2}... differences: {diff}'.format(
                    path=path,
                    host1=zk_hosts[idx1],
                    host2=zk_hosts[idx2],
                    diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in sorted(differences)])
                )
            )
            
        # Check that the znodes have a consistent ephemeral owner session id.
        znode_ephemeral_sets = [frozenset([getattr(host_results[idx][1], 'ephemeralOwner', None)]) for idx in range(len(host_results)) if not isinstance(host_results[idx], Exception)]
        comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(znode_ephemeral_sets)), repeat=2) if pair[0] != pair[1]}
        for idx1, idx2 in comparisons:
            # Set comparison to determine differences between the two hosts
            differences = znode_ephemeral_sets[idx1] ^ znode_ephemeral_sets[idx2]
            if not differences:
                continue
                
            errors.append(
                'ephemeral path [{path}] contains inconsistent ephemeral owner for host:{host1} and host:{host2}... differences: {diff}'.format(
                    path=path,
                    host1=zk_hosts[idx1],
                    host2=zk_hosts[idx2],
                    diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in sorted(differences)])
                )
            )
            
    if not errors:
        log.debug('%s.%s encountered no errors' % (__name__, check_ephemeral_znode_consistency.__name__))
    
    return errors
    
def check_ephemeral_sessions_fast(zk_client):
    """
    Fast ephemeral session check, ensure all ephemeral paths contain valid ephemeral 
    znodes with valid sessions.  The check is performed for each Zookeeper host
    
    This is a fast version, because instead of exhaustively walking all paths to discover all
    ephemerals, it uses 'dump' output to make assumptions about znode paths that contain ephemerals.
    
    :param zk_client: Zookeeper connection object (KazooClient instance or subclass of)
                           start() will be called internally when the connection is used.
                           The connection instance should be configured with the hosts that are
                           members of the ensemble.
    """
    # Connect to each Zookeeper Host
    clients = kazoo_clients_from_client(zk_client)
    kazoo_clients_connect(clients)
    
    children_results = get_ephemeral_paths_children_per_host(zk_client)
    errors = get_async_call_per_host_errors(zk_client, children_results)
    child_paths = get_ephemeral_paths_children_per_host_paths(children_results)
    
    
    # Get connection/session information
    conn_results = multi_admin_command(zk_client, b'cons')
    conn_data = map(parse_admin_cons, conn_results)
    conn_data = list(itertools.chain.from_iterable(conn_data))
    # Get a set() of all valid zookeeper sessions as integers
    valid_sessions = {con.get('sid') for con in conn_data if 'sid' in con}
    log.debug('found %d active sessions across %d ensemble members' % (len(valid_sessions), len(clients)))

    def call(client, znode):
        return client.get_async(znode)
        
    znode_results = get_async_call_per_host(zk_client, child_paths, call)

    for path, host_results in six.viewitems(znode_results):
        for host_idx, result in six.viewitems(host_results):
            if isinstance(result, Exception):
                errors.append(
                    "error from host: %s, path: %s, error: %s" % (zk_client.hosts[host_idx], path, str(result))
                )
            else:
                content, stats = result
                ephemeral_session = getattr(stats, 'ephemeralOwner', None)
                if not ephemeral_session:
                    continue
                
                if ephemeral_session not in valid_sessions:
                    errors.append(
                        "error from host: %s, ephemeral path: %s, session-id: [%s] does not exist on any Zookeeper server" % (
                            zk_client.hosts[host_idx], 
                            path,
                            ephemeral_session
                        )
                    )
                else:
                    log.debug('host %s path %s has valid session: %d' % (zk_client.hosts[host_idx], path, ephemeral_session))
        
    if not errors:
        log.debug('%s.%s encountered no errors' % (__name__, check_ephemeral_sessions_fast.__name__))
        
    return errors
    
LIVE_NODES_PATH = '/live_nodes'
def get_solr_session_ids(zk_client):
    """
    Find client sessions across servers that are solr servers
    """
    # query live-nodes, to get sessions that belong to Solr hosts.
    def call(client, znode):
        return client.get_children_async(znode)
        
    children_results = get_async_call_per_host(zk_client, ['/live_nodes'], call)
    
    errors = []
    if not children_results:
        raise RuntimeError('No live nodes exist on the %d zookeeper hosts checked' % len(zk_client.hosts))
        
    live_nodes = sorted(set(itertools.chain.from_iterable(children_results[LIVE_NODES_PATH].values())))
    live_nodes = [znode_path_join([LIVE_NODES_PATH, node]) for node in live_nodes if not isinstance(node, Exception)]
    
    def call(client, znode):
        return client.get_async(znode)
        
    live_results = get_async_call_per_host(zk_client, live_nodes, call)
    
    if not live_results:
        raise ValueError('znode get() for live_nodes failed to return any results, input: %s' % pformat(live_nodes))
    
    errors.extend(get_async_call_per_host_errors(zk_client, live_results))
    
    live_node_sessions = [[getattr(stats, 'ephemeralOwner', None) for content, stats in live_node.values()] for live_node in live_results.values()]
    live_node_sessions = sorted(set(itertools.chain.from_iterable(live_node_sessions)))
    live_node_sessions = [session_id for session_id in live_node_sessions if session_id is not None]
    
    if errors and not live_node_sessions:
        raise RuntimeError(errors)
    elif errors:
        log.warn(errors)
    
    return live_node_sessions
    
    
def check_solr_live_nodes(zk_client):
    """
    Check that live nodes are all present and consistent
    
    If a collection/replica refers to a node not in the live-nodes list, then thats a problem.
    """
    def call(client, znode):
        return client.get_children_async(znode)
        
    children_results = get_async_call_per_host(zk_client, ['/live_nodes'], call)
    
    errors = []
    if not children_results:
        errors.append('No live nodes exist on the %d zookeeper hosts checked' % len(zk_client.hosts))
        
    
    
def check_solr_administration(zk_client):
    """
    Ensure the solr administrative page is reachable / responsive.
    """
    
def check_solr_query_handler(zk_client):
    """
    Ensure the solr query handlers are responsive
    """
    
def check_solr_cluster_status(zk_client):
    """
    Ensure the solr cluster status reports no downed replicas, etc.
    """
    
def check_collection_state(zk_client):
    """
    Check to ensure all state.json files are present, and contain no down states.
    """
    
    
def check_collections_consistency(zk_client):
    """
    Check that collections are consistent between solr and zookeeper
    """
    
def check_queue_sizes(zk_client):
    """
    For the most part queues should be empty.  If they contain more than a given number of 
    entries, return information.
    """