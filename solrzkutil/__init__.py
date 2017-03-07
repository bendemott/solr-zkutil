#!/usr/bin/python
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import time
import argparse
from datetime import datetime, timedelta, tzinfo
from textwrap import dedent
import json
from random import choice
import webbrowser
import logging
from os.path import expanduser, expandvars, dirname, exists, join
log = logging.getLogger()
logging.basicConfig()

import pendulum
import six
from six.moves import input
from tzlocal import get_localzone
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
from kazoo.handlers.threading import KazooTimeoutError
import colorama
from colorama import Fore, Back, Style

__application__ = 'solr-zkutil'

COMMANDS = {
    # key: cli-value
    # do not change the keys, but you may freely change the values of the tuple, to modify
    # the command or description.
    'solr': ('live-nodes', 'List Solr Live Nodes from ZooKeeper'),
    'clusterstate': ('clusterstate', 'List Solr Collections and Nodes'),
    'watch': ('watch', 'Watch a ZooKeeper Node for Changes'),
    'test': ('test', 'Test Each Zookeeper Ensemble node for replication and client connectivity'), # TODO
    'status': ('stat', 'Check ZooKeeper ensemble status'),
    'config': ('config', 'Show connection strings, or set environment configuration'),
    'admin': ('admin', 'Execute a ZooKeeper administrative command'),
    'ls': ('ls', 'List a ZooKeeper Node')
}

CONFIG_DIRNAME = __application__

HEADER_STYLE = Back.CYAN + Fore.WHITE + Style.BRIGHT
HEADER_JUST = 10
TITLE_STYLE = Fore.CYAN + Style.BRIGHT
INFO_STYLE = Fore.YELLOW + Style.BRIGHT
ERROR_STYLE = Back.WHITE + Fore.RED + Style.BRIGHT
INPUT_STYLE = Fore.WHITE + Style.BRIGHT
BLUE_STYLE = Fore.BLUE + Style.BRIGHT
DIFF_STYLE = Fore.MAGENTA + Style.BRIGHT
STATS_STYLE = Fore.MAGENTA + Style.BRIGHT
GREEN_STYLE = Fore.GREEN + Style.BRIGHT

ZK_LIVE_NODES = '/live_nodes'
ZK_CLUSTERSTATE = '/clusterstate.json'

MODE_LEADER = 'leader'

# the first event will always be triggered immediately to show the existing state of the node
# instead of saying 'watch event' tell the user we are just displaying initial state.
WATCH_COUNTER = 0

ZK_ADMIN_CMDS = {
    'conf': {
        'help': 'Print details about serving configuration.',
        'example': '',
        'version': '3.3.0',
    },
    'cons': {
        'help': ('List full connection/session details for all clients connected to this server. '
            'Includes information on numbers of packets received/sent, session id, operation '
            'latencies, last operation performed, etc...'),
        'example': '',
        'version': '3.3.0',
    },
    'crst':{
        'help': 'Reset connection/session statistics for all connections.',
        'example': '',
        'version': '3.3.0',
    },
    'dump':{
        'help': 'Lists the outstanding sessions and ephemeral nodes. This only works on the leader.',
        'example': '',
        'version': '',
    },
    'envi':{
        'help': 'Print details about serving environment',
        'example': '',
        'version': '',
    },
    'ruok':{
        'help': 'Tests if server is running in a non-error state. The server will respond with imok if it is running. Otherwise it will not respond at all.',
        'example': '',
        'version': '',
    },
    'srst':{
        'help': 'Reset server statistics.',
        'example': '',
        'version': '',
    },
    'srvr':{
        'help': 'Lists full details for the server.',
        'example': '',
        'version': '3.3.0',
    },
    'stat':{
        'help': 'Lists brief details for the server and connected clients.',
        'example': '',
        'version': '',
    },
    'wchs':{
        'help': 'Lists brief information on watches for the server.',
        'example': '',
        'version': '3.3.0',
    },
    'wchc':{
        'help': 'Lists detailed information on watches for the server, by session. (may be expensive)',
        'example': '',
        'version': '3.3.0',
    },
    'dirs':{
        'help': 'Shows the total size of snapshot and log files in bytes',
        'example': '',
        'version': '3.5.1',
    },
    'wchp':{
        'help': 'Lists detailed information on watches for the server, by path. This outputs a list of paths (znodes) with associated sessions.',
        'example': '',
        'version': '3.3.0',
    },
    'mntr': {
        'help': 'Outputs a list of variables that could be used for monitoring the health of the cluster.',
        'example': '3.4.0'
    },
    'isro':{
        'help': 'Tests if server is running in read-only mode. The server will respond with "ro" if in read-only mode or "rw" if not in read-only mode.',
        'example': '',
        'version': '3.4.0',
    },
    'gtmk':{
        'help': 'Gets the current trace mask as a 64-bit signed long value in decimal format. See stmk for an explanation of the possible values.',
        'example': '',
        'version': '',
    },
    'stmk':{
        'help': 'Sets the current trace mask. The trace mask is 64 bits, where each bit enables or disables a specific category of trace logging on the server.',
        'example': '',
        'version': '',
    },
}

ZNODE_DEBUG_ATTRS = [
    'aversion',
    'cversion',
    'version',
    'numChildren',
    'ctime', 
    'mtime',   
    'czxid',
    'mzxid',
    'pzxid',
    'dataLength',
    'ephemeralOwner',
]


NEW_TAB = 2

def config_path():
    conf = None
    if os.name == 'nt':
        conf = os.path.expandvars("%%appdata%%/.%s/environments.json" % CONFIG_DIRNAME)
    else:
        conf = os.path.expanduser("~/.%s/environments.json" % CONFIG_DIRNAME)
    return conf
    
    
def config():
    conf = config_path()

    if not exists(conf):
        if not exists(dirname(conf)):
            os.makedirs(dirname(conf))

        open(conf, mode='w').write(dedent('''
        {
            "DEV": "localhost:2181",
            "QA": "localhost:2181",
            "PILOT": "localhost:2181",
            "PROD": "localhost:2181"
        }
        '''))


    return json.loads(open(conf, mode='r').read().strip())


def style_header(text, width = 0):
    if not text:
        return ''
    width = max(len(text) + HEADER_JUST * 2, width)
    pad = ' ' * width
    output = '\n%s%s\n%s\n%s%s\n' % (HEADER_STYLE, pad, text.center(width), pad, Style.RESET_ALL)
    return output


def style_text(text, styles, ljust=0, rjust=0, cen=0, lpad=0, rpad=0, pad=0, char=' ', restore=''):
    if not text:
        return ''

    # Ensure we have unicode in both python 2/3
    text    = six.text_type(text)
    styles  = six.text_type(styles)
    char    = six.text_type(char)
    restore = six.text_type(restore)
    reset_all = six.text_type(Style.RESET_ALL)
    
    style = ''.join(styles)
    text = text.ljust(ljust, char)
    text = text.rjust(rjust, char)
    text = text.center(cen, char)
    text = char*(lpad+pad) + text + char*(rpad+pad)
    
    return '%s%s%s%s' % (style, text, reset_all, restore)
    #return style + text + Style.RESET_ALL + restore


def style_multiline(text, styles, ljust=0, rjust=0, cen=0, lpad=0, rpad=0, pad=0, char=' '):
    if not text:
        return ''
    lines = text.split('\n')
    fmt_text = ''
    for text in lines:
        text = style_text(text, styles, ljust, rjust, cen, lpad, rpad, pad, char)
        fmt_text += text + '\n'
    return fmt_text

    
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

    
def parse_zk_hosts(zookeepers, all_hosts=False, leader=False):
    """
    Returns [host1, host2, host3]
    """
    zk_hosts, root = zookeepers.split('/') if len(zookeepers.split('/')) > 1 else (zookeepers, None)
    zk_hosts = zk_hosts.split(',')
    root = '/'+root if root else ''
    
    all_hosts_list = [h+root for h in zk_hosts]
    
    if leader:
        zk_hosts = [get_leader(zk_hosts)]
        if not zk_hosts:
            raise RuntimeError('no leader available, from connections given')
    # make a list of each host individually, so they can be queried one by one for statistics.
    elif all_hosts:
        zk_hosts = all_hosts_list
    # otherwise pick a single host to query by random.
    else:
        zk_hosts = [choice(zk_hosts) + root]
        
    return zk_hosts

def update_config(configuration=None, add=None):
    """
    Update the environments configuration on-disk.
    """
    existing_config = config()
    conf = config_path()
    print(style_header('Zookeeper Environments'))
    print("")
    print(style_text('config:', TITLE_STYLE, pad=2), end='')
    print(style_text(conf, INPUT_STYLE))
    print(style_multiline(json.dumps(existing_config, indent=4, sort_keys=True), INFO_STYLE, lpad=4))

    if not configuration and not add:
        return

    new_config = existing_config
    if configuration:
        new_config = configuration
    if add:
        new_config.update(add)

    new_config = json.dumps(new_config, indent=4, sort_keys=True)

    print("")
    print(style_text('new config:', TITLE_STYLE, pad=2))
    print(style_multiline(new_config, INFO_STYLE, lpad=4))
    print("")

    # Get permission to replace the existing configuration.
    if input(style_text("Replace configuration? (y/n): ", INPUT_STYLE)).lower() not in ('y', 'yes'):
        print("  ...Cancel")
        return

    open(conf, mode='w').write(new_config)
    print(style_text('  ...Saved', INPUT_STYLE, pad=2))


def clusterstate(zookeepers, all_hosts, node='clusterstate.json'):
    """
    Print clusterstatus.json contents
    """
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=all_hosts)

    print('')

    # we'll keep track of differences for this node between zookeepers.
    # because zookeeper keeps all nodes in-sync, there shouldn't be differences between the
    # nodes... but there might be if you are having replication problems.
    
    first_state = None
    for host in zk_hosts:
        # connect to zookeeper
        zk = KazooClient(hosts=host, read_only=True)
        try:
            zk.start()
        except KazooTimeoutError as e:
            print('ZK Timeout host: [%s], %s' % (host, e))
            continue

        # If the node doesn't exist... just let the user know.
        if not zk.exists(node):
            node_str = style_text(node, BLUE_STYLE, restore=ERROR_STYLE)
            zk_str = style_text(host, BLUE_STYLE, restore=ERROR_STYLE)
            print(style_text('No node [%s] on %s' % (node_str, zk_str), ERROR_STYLE))
            continue

        print(style_header('Response From: %s [%s]' % (host, node)))
            
        state = bytes.decode(zk.get(node)[0])
        
        if not first_state:
            first_state = state
           
        lines_1 = first_state.split('\n')
        lines_2 = state.split('\n')
        
        # Print the content of the file, highlighting lines that do not match between hosts.
        for idx, line in enumerate(lines_2):
            if len(lines_1)-1 < idx or line != lines_1[idx]:
                style = DIFF_STYLE
            else:
                style = INFO_STYLE
                
            print(style_text(line, style, lpad=4))
            
        zk.stop()
 
    
def show_node(zookeepers, node, all_hosts=False, leader=False, debug=False, interactive=False):
    """
    Show a zookeeper node on one or more servers.
    If the node has children, the children are displayed,
    If the node doesn't have children, the contents of the node are displayed.
    If leader is specified, only the leader is queried for the node
    If all_hosts is specified, each zk host provided is queried individually... if the results 
    are different between nodes, the child nodes that are different will be highlighted.
    
    returns children of the requested node.
    """
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=all_hosts, leader=leader)

    # we'll keep track of differences for this node between zookeepers.
    # because zookeeper keeps all nodes in-sync, there shouldn't be differences between the
    # nodes... but there might be if you are having replication problems.
    all_children = set()

    for host in zk_hosts:
        # connect to zookeeper
        zk = KazooClient(hosts=host, read_only=True)
        try:
            zk.start()
        except KazooTimeoutError as e:
            print('ZK Timeout host: [%s], %s' % (host, e))
            continue

        print('')
        
        # If the node doesn't exist... just let the user know.
        if not zk.exists(node):
            node_str = style_text(node, BLUE_STYLE, restore=ERROR_STYLE)
            zk_str = style_text(host, BLUE_STYLE, restore=ERROR_STYLE)
            print(style_text('No node [%s] on %s' % (node_str, zk_str), ERROR_STYLE, pad=2))
            continue

        if len(zk_hosts) == 1:
            print(style_header('Response From: %s [%s]' % (host, node)))
        else:
            print(style_text('Response From: %s [%s]' % (host, node), HEADER_STYLE, pad=2))
            
        # Query ZooKeeper for the node.
        content, zstats = zk.get(node)
        #  print(dir(zstats))
        # print(getattr(zstats, 'czxid'))
        
        # --- Print Node Stats -------------------------
        znode_unix_time = zstats.mtime / 1000
        # 
        # local_timezone = time.tzname[time.localtime().tm_isdst] DO NOT USE THIS
        is_dst = time.daylight and time.localtime().tm_isdst
        offset_hour = time.altzone / 3600 if is_dst else time.timezone / 3600
        timezone = 'Etc/GMT%+d' % offset_hour
        mod_time = pendulum.fromtimestamp(znode_unix_time, timezone)
        mod_time = mod_time.in_timezone(timezone)
        local_time_str = mod_time.to_day_datetime_string()


        if debug:
            dbg_rjust = max(map(len, ZNODE_DEBUG_ATTRS))
            print(style_text("Node Stats:", TITLE_STYLE, lpad=2))
            for attr_name in ZNODE_DEBUG_ATTRS:
                attr_val = getattr(zstats, attr_name)
                if 'time' in attr_name and attr_val > 1:
                    attr_val = pendulum.fromtimestamp(int(attr_val) / 1000, timezone).in_timezone(timezone).to_day_datetime_string()
                print(style_text(attr_name, STATS_STYLE, lpad=4, rjust=dbg_rjust), style_text(attr_val, INPUT_STYLE))
        else:
            print(style_text('Modified:', STATS_STYLE, lpad=2, rjust=9), style_text(local_time_str, INPUT_STYLE))
            print(style_text('Version:', STATS_STYLE, lpad=2, rjust=9), style_text(str(zstats.version), INPUT_STYLE))
        print('')


        # --- Print Child Nodes, or Node Content -------
        if not zstats.numChildren:
            zcontent = bytes.decode(content or b'')
            if zcontent:
                print(style_text("Contents:", TITLE_STYLE, lpad=2))
                print(style_multiline(zcontent, INFO_STYLE, lpad=4))
            else:
                print(style_text("... No child nodes", INFO_STYLE, lpad=2))
        else:
            children = zk.get_children(node)
            children.sort()
            cwidth = max([len(c) for c in children])
            print(style_text("Child Nodes:", TITLE_STYLE, lpad=2))
            for ch in children:
                child_path = node+ch if node.endswith('/') else node+'/'+ch
                _, czstats = zk.get(child_path)
                if all_children and ch not in all_children:
                    # if this child is unique / different to this zk host, color it differently.
                    print(style_text(ch, INPUT_STYLE, lpad=4, ljust=cwidth), end='')
                else:
                    print(style_text(ch, INFO_STYLE, lpad=4, ljust=cwidth), end='')
                    
                mod_ver = czstats.version or czstats.cversion
                print(style_text('v:', STATS_STYLE, lpad=3), style_text(str(mod_ver), INPUT_STYLE, ljust=3), end='')
                print(style_text('eph:', STATS_STYLE, lpad=3), style_text('yes' if czstats.ephemeralOwner else 'no', INPUT_STYLE), end='')
                
                mod_datetime = datetime.utcfromtimestamp(czstats.mtime / 1000)
                mod_elapsed = datetime.utcnow() - mod_datetime
                if mod_elapsed >= timedelta(hours=48):
                    mod_style = ''
                elif mod_elapsed >= timedelta(hours=2):
                    mod_style = INPUT_STYLE
                elif mod_elapsed >= timedelta(minutes=10):
                    mod_style = GREEN_STYLE
                elif mod_elapsed >= timedelta(minutes=1):
                    mod_style = INFO_STYLE
                else:
                    mod_style =  STATS_STYLE
                    
                if mod_datetime.year != 1970:
                    mod_desc = pendulum.fromtimestamp(czstats.mtime / 1000, 'UTC').diff_for_humans()
                else:
                    mod_desc = 'none'
                    
                print(style_text('mod:', STATS_STYLE, lpad=3), style_text(mod_desc, mod_style))
                
            zk.stop()
            all_children = all_children | set(children)
                
 
        
    return list(all_children)

    
def watch(zookeepers, node, leader=False):
    """
    Watch a particular zookeeper node for changes.
    """
    
    zk_hosts = parse_zk_hosts(zookeepers, leader=leader)[0]

    def my_listener(state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            print(style_text('Connection Lost', ERROR_STYLE, pad=2))
        elif state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            print(style_text('Connection Suspended', ERROR_STYLE, pad=2))
        else:
            # Handle being connected/reconnected to Zookeeper
            # what are we supposed to do here?
            print(style_text('Connected/Reconnected', INFO_STYLE, pad=2))

    zk = KazooClient(hosts=zk_hosts, read_only=True)
    try:
        zk.start()
    except KazooTimeoutError as e:
        print('ZK Timeout host: [%s], %s' % (host, e))
      
    zk_ver = '.'.join(map(str, zk.server_version()))
    zk_host = zk.hosts[zk.last_zxid]
    zk_host = ':'.join(map(str, zk_host))

    zk.add_listener(my_listener)

    # check if the node exists ...
    if not zk.exists(node):
        node_str = style_text(node, BLUE_STYLE, restore=ERROR_STYLE)
        zk_str = style_text(zk_host, BLUE_STYLE, restore=ERROR_STYLE)
        print('')
        print(style_text('No node [%s] on %s' % (node_str, zk_str), ERROR_STYLE, pad=2))
        return

    print(style_header('Watching [%s] on %s v%s' % (node, zk_host, zk_ver)))

    # put a watch on my znode
    children = zk.get_children(node)
    
    # If there are children, watch them.
    if children or node.endswith('/'):
        @zk.ChildrenWatch(node)
        def watch_children(children):
            global WATCH_COUNTER
            WATCH_COUNTER += 1
            
            if WATCH_COUNTER <= 1:
                child_watch_str = 'Child Nodes:'
            else:
                child_watch_str = 'Node Watch Event: '
            
            children.sort()
            print('')
            print(style_text(child_watch_str, TITLE_STYLE))
            for ch in children:
                print(style_text(ch, INFO_STYLE, lpad=2))
            print('')

    else:
    # otherwise watch the node itself.
        @zk.DataWatch(node)
        def watch_data(data, stat, event):
            global WATCH_COUNTER
            WATCH_COUNTER += 1
            
            data = data.decode('utf-8')
            
            if WATCH_COUNTER <= 1:
                data_watch_str = 'Content: (%s)' 
            else:
                data_watch_str = 'Data Watch Event: (v%s)'
                
            print('')
            print(style_text(data_watch_str % stat.version, TITLE_STYLE))
            print(style_multiline(data, INFO_STYLE, lpad=2))
            print('')



    CHAR_WIDTH = 60
    counter = 0
    while True:
        # draw a .... animation while we wait, so the user knows its working.
        counter += 1
        if not counter % CHAR_WIDTH:
            print('\r', ' '*CHAR_WIDTH, '\r', end='')

        print(style_text('.', INFO_STYLE), end='')
        time.sleep(0.05)
        
    zk.stop()


def admin_command(zookeepers, command, all_hosts=False, leader=False):
    """
    Execute an administrative command
    """
    command = six.text_type(command) # ensure we have unicode py2/py3
    zk_hosts = parse_zk_hosts(zookeepers, all_hosts=all_hosts, leader=leader)
    
    for host in zk_hosts:
    
        print('')
    
        zk = KazooClient(hosts=host, read_only=True)
        zk.start()
        # Kazoo expects an object with the 'buffer' inteface.
        strcmd = command.encode('utf-8')
        status = zk.command(cmd=strcmd)
        zk_ver = '.'.join(map(str, zk.server_version()))
        zk_host = zk.hosts[zk.last_zxid]
        zk_host = ':'.join(map(str, zk_host))
        
        zk.stop()
        
        if len(zk_hosts) == 1:
            print(style_header('ZK Command [%s] on %s v%s' % (command, zk_host, zk_ver)))
        else:
            print(style_text('ZK Command [%s] on %s v%s' % (command, zk_host, zk_ver), HEADER_STYLE, pad=2))

        print(style_multiline(status, INFO_STYLE, lpad=2))


def cli():
    """
    Build the CLI menu
    """

    def verify_json(arg):
        try:
            data = json.loads(arg)
        except ValueError as e:
            raise argparse.ArgumentTypeError("invalid json: %s" % e)

        return data

    def verify_env(arg):
        try:
            env_config = config()
        except ValueError as e:
            raise argparse.ArgumentTypeError('Cannot read configuration %s' % e)

        if arg not in env_config:
            raise argparse.ArgumentTypeError('Invalid Environment %s ... Valid: [%s]' % (arg, ', '.join(list(env_config))))

        return env_config[arg]



    def verify_zk(arg):
        hosts = arg.split('/')[0]
        hosts = hosts.split(',')

        if ' ' in arg:
            raise argparse.ArgumentTypeError("There should be no spaces between zookeeper hosts: %s" % arg)

        for zk in hosts:
            hostport = zk.split(':')
            if len(hostport) == 1:
                raise argparse.ArgumentTypeError("Port is required for: %s... default: 2181" % zk)
            else:
                _, port = hostport
                if not port.isdigit():
                    raise argparse.ArgumentTypeError("Port must be numeric for: %s" % zk)

        return arg

        
    def verify_add(arg):
        if '=' not in arg:
            raise argparse.ArgumentTypeError("You must use the syntax ENVIRONMENT=127.0.0.1:2181")
        env, zk = arg.split('=')
        verify_zk(zk)

        return {env.strip(): zk.strip()}

        
        
    def verify_node(arg):
        if not arg.startswith('/'):
            raise argparse.ArgumentTypeError("Zookeeper nodes start with /")

        return arg

        
    def verify_cmd(arg):
        if arg.lower() not in ZK_ADMIN_CMDS:
            raise argparse.ArgumentTypeError("Invalid command '%s'... \nValid Commands: %s" % (arg, '\n    '.join(ZK_ADMIN_CMDS)))

        return arg.lower()

        
    # Top level parser
    parser = argparse.ArgumentParser(prog=__application__)
    subparsers = parser.add_subparsers(help='--- available sub-commands ---', dest='command')

    try:
        env_config = config()
    except ValueError:
        env_config = {}


    zk_argument = {
        'args': ['-z', '--zookeepers'],
        'kwargs': {
            'required': False,
            'default': None,
            'type': verify_zk,
            'help': ('Zookeeper connection string, with optional root... \n'
                    'eg. 127.0.0.1:2181 or 10.10.1.5:2181/root \n'
                    'NOTE: --zookeepers or --env must be specified!')
        }
    }

    env_argument = {
        'args': ['-e', '--env'],
        'kwargs': {
            'required': False,
            'default': None,
            'type': verify_env,
            'help': ('Connect to zookeeper using one of the configured environments. \n'
                    'Note: to view or modify config use the "%s" sub-command. \n'
                    'Configured Environments: [%s]' % (COMMANDS['config'][0], ', '.join(list(env_config))))
        }
    }

    all_argument = {
        'args': ['-a', '--all-hosts'],
        'kwargs': {
            'default': False,
            'required': False,
            'action': 'store_true',
            'help': 'Show response from all zookeeper hosts'
        }
    }
    
    leader_argument = {
        'args': ['-l', '--leader'],
        'kwargs': {
            'default': False,
            'required': False,
            'action': 'store_true',
            'help': 'Query ensemble leader only'
        }
    }
    
    debug_argument = {
        'args': ['--debug'],
        'kwargs': {
            'default': False,
            'required': False,
            'action': 'store_true',
            'help': 'Show debug stats'
        }
    }


    # -- SOLR - LIVE NODES -----------
    cmd, about = COMMANDS['solr']
    solr = subparsers.add_parser(cmd, help=about)
    solr.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    solr.add_argument(*env_argument['args'], **env_argument['kwargs'])
    solr.add_argument(*all_argument['args'], **all_argument['kwargs'])
    solr.add_argument('-b', '--browser', default=False, required=False,
        action='store_true', help='Open solr-admin in web-browser for resolved host')
    solr.add_argument(*leader_argument['args'], **leader_argument['kwargs'])

    # -- SOLR - CLUSTERSTATE -------
    cmd, about = COMMANDS['clusterstate']
    cluster = subparsers.add_parser(cmd, help=about)
    cluster.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    cluster.add_argument(*env_argument['args'], **env_argument['kwargs'])
    cluster.add_argument(*all_argument['args'], **all_argument['kwargs'])

    # -- WATCH ----------------------
    cmd, about = COMMANDS['watch']
    watches = subparsers.add_parser(cmd, help=about)
    watches.add_argument('node', help='Zookeeper node', type=verify_node)
    watches.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    watches.add_argument(*env_argument['args'], **env_argument['kwargs'])
    watches.add_argument(*leader_argument['args'], **leader_argument['kwargs'])


    # -- LS -------------------------
    cmd, about = COMMANDS['ls']
    ls = subparsers.add_parser(cmd, help=about)
    ls.add_argument('node', help='Zookeeper node', type=verify_node) # positional argument
    ls.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    ls.add_argument(*env_argument['args'], **env_argument['kwargs'])
    ls.add_argument(*all_argument['args'], **all_argument['kwargs'])
    ls.add_argument(*leader_argument['args'], **leader_argument['kwargs'])
    ls.add_argument(*debug_argument['args'], **debug_argument['kwargs'])

    # -- STATUS ---------------------
    cmd, about = COMMANDS['status']
    status = subparsers.add_parser(cmd, help=about)
    status.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    status.add_argument(*env_argument['args'], **env_argument['kwargs'])
    status.add_argument(*leader_argument['args'], **leader_argument['kwargs'])

    # -- ADMIN ---------------------
    cmd, about = COMMANDS['admin']
    admin = subparsers.add_parser(cmd, help=about)
    admin.add_argument('cmd', help='ZooKeeper Administrative Command', type=verify_cmd)
    admin.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    admin.add_argument(*env_argument['args'], **env_argument['kwargs'])
    admin.add_argument(*all_argument['args'], **all_argument['kwargs'])
    admin.add_argument(*leader_argument['args'], **leader_argument['kwargs'])

    # -- CONFIG ---------------------
    cmd, about = COMMANDS['config']
    envs = subparsers.add_parser(cmd, help=about)
    envs.add_argument('-c',  '--configuration', default=None, required=False, type=verify_json,
        help='Set the environments configuration located at %s, string passed must be valid json ' % config_path())
    envs.add_argument('-a',  '--add', default=None, required=False, type=verify_add,
        help=('add/update an environment variable using the syntax KEY=VALUE,\n'
        'eg. DEV=zk01.dev.com:2181,zk02.dev.com:2181'))

    return parser


def main(argv=None):

    colorama.init(autoreset=True) # initialize color handling for windows terminals.

    parser = cli()
    args = vars(parser.parse_args(argv[1:]))

    cmd = args['command']

    if (('zookeepers' in args and 'env' in args)
        and not any((args['env'], args['zookeepers']))):
        print("    'zookeepers', or 'env' argument is required", end='')
        print('  ', style_text("Add --help to your command for help", ERROR_STYLE, pad=1))
        return

    if args.get('env') and not args.get('zookeepers'):
        # when env is specified and valid, but zookeepers is not
        # env should have been resolved to a zookeeper host string.
        args['zookeepers'] = args['env']
        

    # -- COMMAND HANDLERS --------------------------------------------------------------------------
    if cmd == COMMANDS['solr'][0]:
        hosts = show_node(zookeepers=args['zookeepers'], node=ZK_LIVE_NODES, all_hosts=args['all_hosts'], leader=args['leader'])
        if args.get('browser') and hosts:
            solr_admin = choice(hosts).replace('_solr', '/solr')
            # C:\Users\Scott\AppData\Local\Google\Chrome\Application\chrome.exe
            # webbrowser._tryorder
            webbrowser.get().open('http://'+solr_admin, new=NEW_TAB, autoraise=True)
            
    elif cmd == COMMANDS['clusterstate'][0]:
        clusterstate(zookeepers=args['zookeepers'], all_hosts=args['all_hosts'])

    elif cmd == COMMANDS['ls'][0]:
        show_node(zookeepers=args['zookeepers'], node=args['node'], all_hosts=args['all_hosts'], leader=args['leader'], debug=args['debug'])

    elif cmd == COMMANDS['watch'][0]:
        watch(zookeepers=args['zookeepers'], node=args['node'], leader=args['leader'])

    elif cmd == COMMANDS['config'][0]:
        update_config(configuration=args['configuration'], add=args['add'])

    elif cmd == COMMANDS['status'][0]:
        # TODO improve this command so it is a combination of mntr, stat, cons, and ruok
        admin_command(zookeepers=args['zookeepers'], command='stat', leader=args['leader'])

    elif cmd == COMMANDS['admin'][0]:
        admin_command(zookeepers=args['zookeepers'], command=args['cmd'], all_hosts=args['all_hosts'], leader=args['leader'])
    else:
        parser.print_help()

    print("")


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit('\n')