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
import sys
import six
import traceback

from kazoo.retry import KazooRetry
from kazoo.client import KazooClient

from solrzkutil.parser import (parse_admin_dump, 
                               parse_admin_cons, 
                               parse_admin_wchp, 
                               parse_admin_wchc)
from solrzkutil.util import (netcat, 
                             parse_zk_hosts, 
                             kazoo_clients_from_client, 
                             kazoo_clients_connect, 
                             kazoo_client_cache_enable)

from pprint import pprint
ZNODE_PATH_SEPARATOR = '/'

def connect_to_zookeeper(zookeepers):
    try:
        c = KazooClient(zookeepers)
        return c
    except Exception as e:
        output = [get_exception_traceback()]
        log.error(output)

def multi_admin_command(zk_client, command):
    """
    Executes an administrative command over multiple zookeeper nodes in a session-less manner 
    using threading.
    
    Using threading not only speeds up the total time taken to query the remote Zookeeper hosts, 
    it also ensures the most similar and real-time results from the servers.
    
    :param zk_client: Zookeeper connection object (KazooClient instance or subclass of)
                           The connection instance should be configured with the hosts that are
                           members of the ensemble.
    :param command: the administrative command to execute on each host within ``zk_client.hosts``
    """
    # TODO handle exceptions from the client, exceptions should 
    #   return None or the Exception object. 
    
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
    
    Note that you should-not/cannot use path functions in Python to parse znode paths as they will not
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
    comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(ephemerals_compare)), repeat=2) if pair[0] != pair[1]}
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

def check_watch_sessions_clients(zk_client):
    """
    Check watch consistency for client-related files, exclude solr hosts from these tests.
    """
    CLIENT_WATCHES = ('/clusterprops.json', '/clusterstate.json', '/aliases.json')
    solr_sessions = get_solr_session_ids(zk_client)
    return check_watch_session_consistency(zk_client, CLIENT_WATCHES, exclude=solr_sessions)
    
def check_watch_sessions_solr(zk_client):
    """
    Check watch consistency for solr sessions only.
    """
    # TODO, in order to verify this you need to know quite a bit of information about collections,
    # active config sets, etc.
    
def check_watch_sessions_valid(zk_client):
    """
    Ensure all watch sessions have a valid connection associated with them.
    """
    # TODO finish me
    errors = []
    zk_hosts = zk_client.hosts
    wchc_results = multi_admin_command(zk_client, b'wchc')
    wchc_result_parsed = [parse_admin_wchc(result) for result in wchc_results]
    

def check_watch_sessions_duplicate(zk_client):
    """
    Ensure no watch session id is represented on more than one server
    This shouldn't happen, and if it does it indicates a hung or invalid watch, which means a session was
    reconnected / reused it chose a different Zookeeper host and the old watch never was cleared.
    Restart the Zookeeper host with the 

    We will use the ``wchc`` administrative command to get watches by session-id for this check.
    """
    # TODO finish me
    errors = []
    zk_hosts = zk_client.hosts
    wchc_results = multi_admin_command(zk_client, b'wchc')
    wchc_result_parsed = [parse_admin_wchc(result) for result in wchc_results]

    # from pprint import pprint
    # pprint(wchc_result_parsed)
    
    #session_watches = {}
    #for host_idx, wchc in enumerate(wchc_result_parsed):


    # First thing to check, do any of the hosts share the same session, this would be a problem.
    comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(zk_hosts)), repeat=2) if pair[0] != pair[1]}
    for idx1, idx2 in comparisons:
        # Set comparison to determine differences between the two hosts
        differences = set(wchc_result_parsed[0].keys()) ^ set(wchc_result_parsed[1].keys())
        if not differences:
            log.debug('host:{host1} and host:{host2} have {watch1ct} and {watch2ct} watches, and no duplicates.'.format(
                host1=zk_hosts[idx1],
                host2=zk_hosts[idx2],
                watch1ct=len(wchc_result_parsed[idx1].keys()),
                watch2ct=len(wchc_result_parsed[idx2].keys())
            ))
            continue
            
        errors.append(
            'duplicate sessions are present on watches for host:{host1} and host:{host2}... duplicates: {diff}'.format(
                host1=zk_hosts[idx1],
                host2=zk_hosts[idx2],
                diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in differences])
            )
        )

    return errors
    
def check_watch_sessions_present(zk_client, sessions, watch_paths):
    """
    Verify that watches exist on all the paths defined for the sessions defined.
    This function will assume you expect exactly 1 watch to exist per session defined.

    For any path in ``watch_paths`` one of the sessions in ``sessions`` must be watching it
    or we have an error.
    
    Note that watches can exist on any Zookeeper server, so we have to check all of the servers
    for watches.
    
    We will use the ``wchc`` administrative command to get watches by session-id for this check.
    """
    # TODO finish me
    errors = []
    zk_hosts = zk_client.hosts
    wchc_results = multi_admin_command(zk_client, b'wchc')
    wchc_result_parsed = [parse_admin_wchc(result) for result in wchc_results]
    
    # combine / merge all of the sessiosns
    session_watches = {}
    for wchc in wchc_result_parsed:
        session_watches.update(wchc)
    
    
def check_watch_session_consistency(zk_client, watch_paths, exclude=None, include=None):
    """
    Verify watches on the given files are consistent.
    
    This function finds situations where a client is watching perhaps 2 files, when it should be
    watching 3.  One of its watches has died or timed out.
    
    :param include: include ONLY these session ids in the check.
    :param exclude: exclude these session ids from the check, useful to exclude yourself, or client
                    other client sessions.
    :param watch_paths: A list of fully qualified znode paths that should have consistent 
                        watch sessions across them.
    
    Should have a consistent set of watches across servers.
    """
    
    '''
    wchp_result_data will be a dictionary, where each top-level key corresponds to
    a znode, and each value is a dictionary, where the indexes are the Zookeeper hosts indexes
    as defined in zk_client.hosts, and the value is the list of session ids for that host, for that 
    znode.
    
    Example::
        
        {
            '/clusterprops.json': {
                host-idx-1: [session-1, session-2, session-3],
                host-idx-2: [session-1, session-2, session-3]
            },
            '/aliases.json': {
               host-idx-1: [session-4, session-5, session-6],
               host-idx-2: [session-4, session-5, session-6]
            }
        }
    '''
    errors = []
    zk_hosts = zk_client.hosts
    wchp_results = multi_admin_command(zk_client, b'wchp')
    wchp_result_parsed = [parse_admin_wchp(result) for result in wchp_results]
    wchp_result_data = defaultdict(dict)
    for path in watch_paths:
        for host_idx in range(len(zk_hosts)):
            if not wchp_result_parsed[host_idx]:
                wchp_result_data[path][host_idx] = []
            else:
                sessions = set(wchp_result_parsed[host_idx].get(path, []))
                if exclude:
                    sessions = sessions - set(exclude)
                if include:
                    sessions = sessions & set(include)
                wchp_result_data[path][host_idx] = sessions

                
    # Check to see if any of the znodes has NO watches across all servers.
    for path, host_sessions in six.viewitems(wchp_result_data):
        # combine all the individual zookeeper hosts results, if this list is empty 
        # then no sessions were returned for this path.
        sessions = itertools.chain.from_iterable(host_sessions.values())
        if not sessions:
            errors.append(
                "no watches exist for znode: %s on %d zookeeper hosts checked" % (path, len(zk_hosts))
            )
    
    # Find comparison sets, the indexes will be indexes of `watch_paths`
    comparisons = {tuple(sorted(pair)) for pair in itertools.product(range(len(watch_paths)), repeat=2) if pair[0] != pair[1]}
    
    # Check to ensure each znode contains the same set of session watches.
    for host_idx in range(len(zk_hosts)):
        for idx1, idx2 in comparisons:
            path1 = watch_paths[idx1]
            path2 = watch_paths[idx2]
            differences = set(wchp_result_data[path1][host_idx]) ^ set(wchp_result_data[path2][host_idx])
            if differences:
                errors.append(
                    '{zk_host} sessions watches do not match across files:{file1} and {file2}... differences: {diff}'.format(
                        zk_host = zk_hosts[host_idx],
                        file1=path1,
                        file2=path2,
                        diff='\n\t' + '\n\t'.join([six.text_type(entry) for entry in differences])
                    )
                )
    
    if not errors:
        log.debug('%s.%s encountered no errors' % (__name__, check_watch_session_consistency.__name__))

    return errors

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
    
    :param zk_client: Zookeeper connection object (KazooClient instance or subclass of)
                           start() will be called internally when the connection is used.
                           The connection instance should be configured with the hosts that are
                           members of the ensemble.
                           
    :param async_result: The response from ``get_async_call_per_host()``
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
                    "error from host: %s for input: [%s], error: (%s) %s" % (
                        hosts[client_idx],
                        arg,
                        exception.__class__.__name__,
                        str(exception)
                    )
                )
                continue
                
    return errors
    
def get_ephemeral_paths_children_per_host_paths(ephemeral_children):
    """
    Get a simple list of unique paths returned by ``get_ephemeral_paths_children_per_host``
    
    :param ephemeral_children: A sequence of absolute paths.
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
    
    :param zk_client: Zookeeper connection object (KazooClient instance or subclass of)
                           start() will be called internally when the connection is used.
                           The connection instance should be configured with the hosts that are
                           members of the ensemble.
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
                exception = result
                # see if this one is an error.
                errors.append(
                    "error from host: %s, path: %s, error: (%s) %s" % (
                        zk_client.hosts[host_idx],
                        path,
                        exception.__class__.__name__,
                        str(exception)
                    )
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
    
    live_node_sessions = [[getattr(content_stats[1], 'ephemeralOwner', None) for content_stats in live_node.values() if not isinstance(content_stats, Exception)] 
                            for live_node in live_results.values()]
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
    # TODO finish me
    def call(client, znode):
        return client.get_children_async(znode)
        
    children_results = get_async_call_per_host(zk_client, [LIVE_NODES_PATH], call)
    
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

def get_exception_traceback():
    ex_type, ex, tb = sys.exc_info()
    traceback.format_tb(tb,10)
    exception_info =  " ** (%s) %s - %s " % (ex_type, ex, ";\n".join(traceback.format_tb(tb,10)))
    del tb
    return exception_info

def check_ensemble_for_complex_errors(zk_client):
    """
    This function does several complex checks: 
        * Checks zookeeper connectivity.
        * Checks ephemeral nodes.
        * Checks watches.
    """
    errors = []

    try:
        errors.extend(check_zookeeper_connectivity(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    try:
        errors.extend(check_ephemeral_sessions_fast(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    try:
        errors.extend(check_ephemeral_znode_consistency(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    try:
        errors.extend(check_ephemeral_dump_consistency(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    try:
        errors.extend(check_watch_sessions_clients(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    try:
        errors.extend(check_watch_sessions_duplicate(zk_client))
    except Exception as e:
        errors.extend([get_exception_traceback()])

    return errors