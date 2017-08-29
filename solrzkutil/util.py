from __future__ import unicode_literals
from __future__ import print_function
import socket
import time
import six
from random import choice 

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.handlers.threading import KazooTimeoutError
    
def get_leader(zk_hosts):
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
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hostname, int(port)))
    sock.settimeout(timeout)
 
    # send the request
    content = six.binary_type(content)
    try:
        sock.sendall(content)
    except socket.timeout as e:
        raise IOError("connect timeout: %s calling [%s] on [%s]" % (e, content, hostname))
    except socket.error as e: # subclass of IOError
        raise IOError("connect error: %s calling [%s] on [%s]" % (e, content, hostname))
 
    response = ''
    started = time.time()
    while 1:
        if time.time() - started >= timeout:
            raise IOError("timedout retrieving data from: %s" % hostname)
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