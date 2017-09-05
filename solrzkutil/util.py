from __future__ import unicode_literals
from __future__ import print_function
import socket
import time
import six
import math
from random import choice 
import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import OperationTimeoutError

log = logging.getLogger(__name__)

CONNECTION_CACHE_ENABLED = True
CONNECTION_CACHE = {}
def kazoo_client_cache_enable(enable):
    """
    You may disable or enable the connection cache using this function.
    The connection cache reuses a connection object when the same connection parameters
    are encountered that have been used previously.  Because of the design of parts of this program
    functionality needs to be independent and uncoupled, which means it needs to establish its own
    connections.
    Connections to Zookeeper are the most time consuming part of most interactions so caching
    connections enables much faster running of tests health checks, etc.
    """
    global CONNECTION_CACHE_ENABLED
    CONNECTION_CACHE_ENABLED = enable
    

def kazoo_client_cache_serialize_args(kwargs):
    '''
    Returns a hashable object from keyword arguments dictionary.
    This hashable object can be used as the key in another dictionary.
    
    :param kwargs: a dictionary of connection parameters passed to KazooClient 
    
    Supported connection parameters::
    
        hosts - Comma-separated list of hosts to connect to (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
        timeout - The longest to wait for a Zookeeper connection.
        client_id - A Zookeeper client id, used when re-establishing a prior session connection.
        handler - An instance of a class implementing the IHandler interface for callback handling.
        default_acl - A default ACL used on node creation.
        auth_data - A list of authentication credentials to use for the connection. 
                    Should be a list of (scheme, credential) tuples as add_auth() takes.
        read_only - Allow connections to read only servers.
        randomize_hosts - By default randomize host selection.
        connection_retry - A kazoo.retry.KazooRetry object to use for retrying the connection to 
                           Zookeeper. Also can be a dict of options which will be used for creating one.
        command_retry - A kazoo.retry.KazooRetry object to use for the KazooClient.retry() method. 
                        Also can be a dict of options which will be used for creating one.
        logger - A custom logger to use instead of the module global log instance.
    '''
    return frozenset(kwargs.items())

def kazoo_client_cache_get(kwargs):
    if CONNECTION_CACHE_ENABLED:
        return CONNECTION_CACHE.get(kazoo_client_cache_serialize_args(kwargs))
    
def kazoo_client_cache_put(kwargs, client):
    global CONNECTION_CACHE
    CONNECTION_CACHE[kazoo_client_cache_serialize_args(kwargs)] = client

def kazoo_clients_connect(clients, timeout=5, continue_on_error=False):
    """
    Connect the provided Zookeeper client asynchronously.
    This is the fastest way to connect multiple clients while respecting a timeout.
    
    :param clients: a sequence of KazooClient objects or subclasses of.
    :param timeout: connection timeout in seconds
    :param continue_on_error: don't raise exception if SOME of the hosts were able to connect
    """
    asyncs = []
    for client in clients:
        # returns immediately
        asyncs.append(client.start_async())

    tstart = time.time()
    while True:
        elapsed = time.time() - tstart
        remaining = math.ceil(max(0, timeout - elapsed))
        connecting = [async for idx, async in enumerate(asyncs) if not clients[idx].connected]
        connected_ct = len(clients) - len(connecting)
        if not connecting:
            # successful - all hosts connected
            return connected_ct
        if not remaining:
            # stop connection attempt for any client that timed out.
            for client in clients:
                if client.connected:
                    continue
                else:
                    client.stop()
                    
            if len(connecting) < len(clients):
                # if some of the clients connected, return the ones that are connected
                msg = 'Connection Timeout - %d of %d clients timed out after %d seconds' % (
                    len(connecting),
                    len(clients),
                    timeout
                )
                if continue_on_error:
                    log.warn(msg)
                    return connected_ct
                else:
                    OperationTimeoutError(msg)
                
                
            raise OperationTimeoutError('All hosts timed out after %d secs' % timeout)
        
        # Wait the remaining amount of time to connect
        # note that this will wait UP TO remaining, but will only wait as long as it takes
        # to connect.
        connecting[0].wait(remaining)
    
def kazoo_clients_from_client(kazoo_client):
    """
    Construct a series of KazooClient connection objects from a single KazooClient instance
    
    A client will be constructed per host within the KazooClient, so if the KazooClient was 
    constructed with 3 hosts in its connection string, 3 KazooClient instanctes will be returned
    
    The class constructed will be the same type as is passed in kazoo_client, this functionality
    is so that this method will work with mocked connection objects or customized subclasses of
    KazooClient.
    """
    # TODO support all connection arguments
    connection_strings = zk_conns_from_client(kazoo_client)
    cls = kazoo_client.__class__
    clients = []
    for conn_str in connection_strings:
        args =  {'hosts': conn_str}
        client = kazoo_client_cache_get(args)
        if not client:
            client = cls(**args)
            kazoo_client_cache_put(args, client)
        clients.append(client)
            
    return clients
    
def get_leader(zk_hosts):
    # TODO refactor me to accept KazooClient object.
    for host in zk_hosts:
    
        zk = KazooClient(hosts=host, read_only=True)
        try:
            zk.start()
        except KazooTimeoutError as e:
            print('ZK Timeout host: [%s], %s' % (host, e))
            continue
            
        properties_str = zk.command(cmd=b'srvr')
        properties = properties_str.split('\n')
        for line in properties:
            if not line.strip().lower().startswith('mode:'):
                continue
            key, val = line.split(':')
            if val.strip().lower() == MODE_LEADER:
                return host
        
        zk.stop()
        
    raise RuntimeError('no leader available, from connections given')
        
def get_server_by_id(zk_hosts, server_id):
    # TODO refactor me to accept KazooClient object.
    if not isinstance(server_id, int):
        raise ValueError('server_id must be int, got: %s' % type(server_id))

    for host in zk_hosts:
    
        zk = KazooClient(hosts=host, read_only=True)
        try:
            zk.start()
        except KazooTimeoutError as e:
            print('ZK Timeout host: [%s], %s' % (host, e))
            continue
            
        properties_str = zk.command(cmd=b'conf')
        properties = properties_str.split('\n')
        for line in properties:
            if not line.strip().lower().startswith('serverid='):
                continue
            key, val = line.split('=')
            val = int(val)
            if val == server_id:
                return host
            continue 
        
        zk.stop()
        
    raise ValueError("no host available with that server id [%d], from connections given" % server_id)

def zk_conn_from_client(kazoo_client):
    """
    Make a Zookeeper connection string from a KazooClient instance
    """
    hosts = kazoo_client.hosts
    chroot = kazoo_client.chroot
    return zk_conn_from_hosts(hosts, chroot)
    
def zk_conns_from_client(kazoo_client):
    """
    Make a Zookeeper connection string per-host from a KazooClient instance
    """
    hosts = kazoo_client.hosts
    chroot = kazoo_client.chroot
    return zk_conns_from_hosts(hosts, chroot)
    
def zk_conn_from_hosts(hosts, chroot=None):
    """
    Make a Zookeeper connection string from a list of (host,port) tuples.
    """
    if chroot and not chroot.startswith('/'):
        chroot = '/' + chroot
    return ','.join(['%s:%s' % (host,port) for host, port in hosts]) + chroot or ''
    
def zk_conns_from_hosts(hosts, chroot=None):
    """
    Make a list of Zookeeper connection strings one her host.
    """
    if chroot and not chroot.startswith('/'):
        chroot = '/' + chroot
    return ['%s:%s' % (host,port) + chroot or '' for host, port in hosts]
    
def parse_zk_conn(zookeepers):
    """
    Parse Zookeeper connection string into a list of fully qualified connection strings.
    """
    zk_hosts, root = zookeepers.split('/') if len(zookeepers.split('/')) > 1 else (zookeepers, None)
    zk_hosts = zk_hosts.split(',')
    root = '/'+root if root else ''
    
    all_hosts_list = [h+root for h in zk_hosts]
    
    return all_hosts_list
    
def parse_zk_hosts(zookeepers, all_hosts=False, leader=False, server_id=None):
    """
    Returns [host1, host2, host3]
    
    Default behavior is to return a single host from the list chosen by random (a list of 1)
    
    :param all_hsots: if true, all hosts will be returned in a list
    :param leader: if true, return the ensemble leader host 
    :param server_id: if provided, return the host with this server id (integer)
    """
    zk_hosts, root = zookeepers.split('/') if len(zookeepers.split('/')) > 1 else (zookeepers, None)
    zk_hosts = zk_hosts.split(',')
    root = '/'+root if root else ''
    
    all_hosts_list = [h+root for h in zk_hosts]
    
    if leader:
        zk_hosts = [get_leader(zk_hosts)]
            
    elif server_id:
        zk_hosts = [get_server_by_id(zk_hosts, server_id)]
            
    # make a list of each host individually, so they can be queried one by one for statistics.
    elif all_hosts:
        zk_hosts = all_hosts_list
        
    # otherwise pick a single host to query by random.
    else:
        zk_hosts = [choice(zk_hosts) + root]
        
    return zk_hosts


def text_type(string, encoding='utf-8'):
    """
    Given text, or bytes as input, return text in both python 2/3
    
    This is needed because the arguments to six.binary_type and six.text_type change based on
    if you are passing it text or bytes, and if you simply pass bytes to 
    six.text_type without an encoding you will get output like: ``six.text_type(b'hello-world')``
    which is not desirable.
    """
    if isinstance(string, six.text_type):
        return six.text_type(string)
    else:
        return six.text_type(string, encoding)

def netcat(hostname, port, content, timeout=5):
    """
    Operate similary to netcat command in linux (nc).
    """
    # SOCK_DGRAM = UDP 

 
    # send the request
    content = six.binary_type(content)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, int(port)))
        sock.settimeout(timeout)
        sock.sendall(content)
        sock.shutdown(socket.SHUT_WR)
    except socket.timeout as e:
        raise IOError("connect timeout: %s calling [%s] on [%s]" % (e, content, hostname))
    except socket.error as e: # subclass of IOError
        raise IOError("connect error: %s calling [%s] on [%s]" % (e, content, hostname))
 
    response = ''
    started = time.time()
    while True:
        if time.time() - started >= timeout:
            raise IOError("timed out retrieving data from: %s" % hostname)
        # receive the response
        try:
            # blocks until there is data on the socket
            msg = sock.recv(1024)
            response += text_type(msg)
        except socket.timeout as e:
            raise IOError("%s calling [%s] on [%s]" % (e, content, hostname))
        except socket.error as e:
            raise IOError("%s calling [%s] on [%s]" % (e, content, hostname))
 
        if len(msg) == 0:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            break

    response = text_type(response)
        
    return response