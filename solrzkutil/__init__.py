#!/usr/bin/python
from __future__ import print_function

import os
import sys
import time
import argparse
from textwrap import dedent
import json
from random import choice
import webbrowser
import logging
from os.path import expanduser, expandvars, dirname, exists
log = logging.getLogger()
logging.basicConfig()

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType

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

ZK_LIVE_NODES = '/live_nodes'
ZK_CLUSTERSTATE = '/clusterstate.json'
ZK_ADMIN_CMDS = '''
conf
cons
crst
dump
envi
ruok
srst
srvr
stat
wchs
wchc
dirs
wchp
mntr
isro
gtmk
stmk
'''.strip().split('\n')

NEW_TAB = 2

def config_path():
    conf = None
    if os.name == 'win32':
        conf = os.path.expandvars("%appdata%/.%s/environments.json" % CONFIG_DIRNAME)
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







def style_header( text, width = 0):
    width = max(len(text) + HEADER_JUST * 2, width)
    pad = ' ' * width
    output = '\n%s%s\n%s\n%s%s\n' % (HEADER_STYLE, pad, text.center(width), pad, Style.RESET_ALL)
    return output


def style_text(text, styles, ljust=0, rjust=0, cen=0, lpad=0, rpad=0, pad=0, char=' ', restore=''):
    style = ''.join(styles)
    text = text.ljust(ljust, char)
    text = text.rjust(rjust, char)
    text = text.center(cen, char)
    text = char*(lpad+pad) + text + char*(rpad+pad)
    return style + text + Style.RESET_ALL + restore

def style_multiline(text, styles, ljust=0, rjust=0, cen=0, lpad=0, rpad=0, pad=0, char=' '):
    lines = text.split('\n')
    style = ''.join(styles)
    fmt_text = ''
    for text in lines:
        text = style_text(text, styles, ljust, rjust, cen, lpad, rpad, pad, char)
        fmt_text += text + '\n'
    return fmt_text



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
    if raw_input(style_text("Replace configuration? (y/n): ", INPUT_STYLE)).lower() not in ('y', 'yes'):
        print("  ...Cancel")
        return

    open(conf, mode='w').write(new_config)
    print(style_text('  ...Saved', INPUT_STYLE, pad=2))

def clusterstate(zookeepers, all_hosts, node='clusterstate.json'):
    """
    Print clusterstatus.json contents
    """
    zk_hosts, root = zookeepers.split('/') if len(zookeepers.split('/')) > 1 else (zookeepers, None)
    zk_hosts = zk_hosts.split(',')
    root = '/'+root if root else ''

    # If we've been asked to show the node for EACH zk node individually iterate through all hosts.
    if all_hosts:
        zk_hosts = [h+root for h in zk_hosts]
    # otherwise pick a single host to query.
    else:
        zk_hosts = [choice(zk_hosts) + root]

    print('')

    # we'll keep track of differences for this node between zookeepers.
    # because zookeeper keeps all nodes in-sync, there shouldn't be differences between the
    # nodes... but there might be if you are having replication problems.
    
    first_state = None
    for host in zk_hosts:
        # connect to zookeeper
        zk = KazooClient(hosts=host)
        zk.start()

        # If the node doesn't exist... just let the user know.
        if not zk.exists(node):
            node_str = style_text(node, BLUE_STYLE, restore=ERROR_STYLE)
            zk_str = style_text(host, BLUE_STYLE, restore=ERROR_STYLE)
            print(style_text('No node [%s] on %s' % (node_str, zk_str), ERROR_STYLE))
            continue

        print(style_header('Response From: %s [%s]' % (host, node)))
            
        state = zk.get(node)[0]
        
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
    
    
def show_node(zookeepers, node, all_hosts=False):
    """
    Show a zookeeper node on one or more servers.
    returns children of the requested node.
    """
    zk_hosts, root = zookeepers.split('/') if len(zookeepers.split('/')) > 1 else (zookeepers, None)
    zk_hosts = zk_hosts.split(',')
    root = '/'+root if root else ''

    # If we've been asked to show the node for EACH zk node individually iterate through all hosts.
    if all_hosts:
        zk_hosts = [h+root for h in zk_hosts]
    # otherwise pick a single host to query.
    else:
        zk_hosts = [choice(zk_hosts) + root]

    print('')

    # we'll keep track of differences for this node between zookeepers.
    # because zookeeper keeps all nodes in-sync, there shouldn't be differences between the
    # nodes... but there might be if you are having replication problems.
    all_children = set()

    for host in zk_hosts:
        # connect to zookeeper
        zk = KazooClient(hosts=host)
        zk.start()

        # If the node doesn't exist... just let the user know.
        if not zk.exists(node):
            node_str = style_text(node, BLUE_STYLE, restore=ERROR_STYLE)
            zk_str = style_text(host, BLUE_STYLE, restore=ERROR_STYLE)
            print(style_text('No node [%s] on %s' % (node_str, zk_str), ERROR_STYLE))
            continue

        children = zk.get_children(node)
        all_children = all_children | set(children)

        print(style_header('Response From: %s [%s]' % (host, node)))

        if not children:
            
            contents = zk.get(node)[0]
            if contents:
                print(style_text("Contents:", TITLE_STYLE, lpad=2))
                print(style_multiline(contents, INFO_STYLE, lpad=4))
            else:
                print(style_text("... No child nodes", INFO_STYLE, lpad=2))

        for ch in children:
            if all_children and ch not in all_children:
                # if this child is unique / different to this zk host, color it differently.
                print(style_text(ch, INPUT_STYLE, lpad=2))
            else:
                print(style_text(ch, INFO_STYLE, lpad=2))

        zk.stop()
                
    return list(all_children)


def watch(zookeepers, node):
    """
    Watch a particular zookeeper node for changes.
    """

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

    zk = KazooClient(hosts=zookeepers)
    zk.start()
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
    if children:
        @zk.ChildrenWatch(node)
        def watch_children(children):
            print('')
            print(style_text("Watch Event: ", TITLE_STYLE))
            for ch in children:
                print(style_text(ch, INFO_STYLE, lpad=2))
            print('')
    else:
    # otherwise watch the node itself.
        @zk.DataWatch(node)
        def watch_data(data, stat, event):
            print('')
            print(style_text("Watch Event: (%s)" % stat.version, TITLE_STYLE))
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


def admin_command(zookeepers, command):
    """
    Execute an administrative command
    """
    zk = KazooClient(hosts=zookeepers)
    zk.start()
    status = zk.command(cmd=command)
    zk_ver = '.'.join(map(str, zk.server_version()))
    zk_host = zk.hosts[zk.last_zxid]
    zk_host = ':'.join(map(str, zk_host))
    
    zk.stop()

    print(style_header('ZK Command [%s] on %s v%s' % (command, zk_host, zk_ver)))
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
            raise argparse.ArgumentTypeError('Invalid Environment %s ... Valid: [%s]' % (arg, ', '.join(env_config.keys())))

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
                host, port = hostport
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
                    'Configured Environments: [%s]' % (COMMANDS['config'][0], ', '.join(env_config.keys())))
        }
    }

    node_argument = {
        'args': ['-n', '--node'],
        'kwargs': {
            'required': True,
            'type': verify_node,
            'help': 'Zookeeper Node'
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


    # -- SOLR - LIVE NODES -----------
    cmd, help = COMMANDS['solr']
    solr = subparsers.add_parser(cmd, help=help)
    solr.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    solr.add_argument(*env_argument['args'], **env_argument['kwargs'])
    solr.add_argument(*all_argument['args'], **all_argument['kwargs'])
    solr.add_argument('-b', '--browser', default=False, required=False,
        action='store_true', help='Open solr-admin in web-browser for resolved host')

    # -- SOLR - CLUSTERSTATE -------
    cmd, help = COMMANDS['clusterstate']
    cluster = subparsers.add_parser(cmd, help=help)
    cluster.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    cluster.add_argument(*env_argument['args'], **env_argument['kwargs'])
    cluster.add_argument(*all_argument['args'], **all_argument['kwargs'])

    # -- WATCH ----------------------
    cmd, help = COMMANDS['watch']
    watch = subparsers.add_parser(cmd, help=help)
    watch.add_argument('node', help='Zookeeper node')
    watch.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    watch.add_argument(*env_argument['args'], **env_argument['kwargs'])


    # -- LS -------------------------
    cmd, help = COMMANDS['ls']
    ls = subparsers.add_parser(cmd, help=help)
    ls.add_argument('node', help='Zookeeper node') # positional argument
    ls.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    ls.add_argument(*env_argument['args'], **env_argument['kwargs'])
    ls.add_argument(*all_argument['args'], **all_argument['kwargs'])

    # -- STATUS ---------------------
    cmd, help = COMMANDS['status']
    status = subparsers.add_parser(cmd, help=help)
    status.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    status.add_argument(*env_argument['args'], **env_argument['kwargs'])

    # -- ADMIN ---------------------
    cmd, help = COMMANDS['admin']
    admin = subparsers.add_parser(cmd, help=help)
    admin.add_argument('cmd', help='ZooKeeper Administrative Command', type=verify_cmd)
    admin.add_argument(*zk_argument['args'], **zk_argument['kwargs'])
    admin.add_argument(*env_argument['args'], **env_argument['kwargs'])
    

    # -- CONFIG ---------------------
    cmd, help = COMMANDS['config']
    envs = subparsers.add_parser(cmd, help=help)
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
        hosts = show_node(zookeepers=args['zookeepers'], node=ZK_LIVE_NODES, all_hosts=args['all_hosts'])
        if args.get('browser'):
            solr_admin = choice(hosts).replace('_solr', '/solr')
            # C:\Users\Scott\AppData\Local\Google\Chrome\Application\chrome.exe
            # webbrowser._tryorder
            webbrowser.get().open('http://'+solr_admin, new=NEW_TAB, autoraise=True)
            
    elif cmd == COMMANDS['clusterstate'][0]:
        clusterstate(zookeepers=args['zookeepers'], all_hosts=args['all_hosts'])

    elif cmd == COMMANDS['ls'][0]:
        show_node(zookeepers=args['zookeepers'], node=args['node'], all_hosts=args['all_hosts'])

    elif cmd == COMMANDS['watch'][0]:
        watch(zookeepers=args['zookeepers'], node=args['node'])

    elif cmd == COMMANDS['config'][0]:
        update_config(configuration=args['configuration'], add=args['add'])

    elif cmd == COMMANDS['status'][0]:
        admin_command(zookeepers=args['zookeepers'], command='stat')

    elif cmd == COMMANDS['admin'][0]:
        admin_command(zookeepers=args['zookeepers'], command=args['cmd'])
    else:
        raise ValueError('unmatched command:', cmd)

    print("")


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit('\n')