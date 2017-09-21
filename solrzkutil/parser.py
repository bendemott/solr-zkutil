import string
import struct
import six
from collections import defaultdict
from dateutil import parser


def remove_symbols(text):
    return text.translate(text.maketrans("",""), string.punctuation)

def int_or_none(text):
    if text is None:
        return

    if isinstance(text, six.integer_types):
        return text
    try:
        num = int(text)
    except TypeError:
        num = None

    return num

def hex_or_none(text):
    if text is None:
        return

    if isinstance(text, six.integer_types):
        return text

    try:
        num = int(text, 16)
    except ValueError:
        num = None

    return num

def parse_zxid(text):
    """
    Parse a zookeeper transaction id into its epoch and number.

    zxid is the global (cluster wide) transaction identifier.
    The upper 32 bits of which are the epoch number (changes when leadership changes)
    and the lower 32 bits which are the xid (transaction id) proper
    """
    if text is None:
        return
        
    if text == '0xffffffffffffffff':
        return None
    
    # Parse as a 64 bit hex int
    zxid = int(text.strip(), 16)

    # convert to bytes
    try:
        zxid_bytes = struct.pack('>q', zxid)
    except struct.error as e:
        raise ValueError("Unable to pack struct, from value: %s, input: %s - %s" % (zxid, text, e))
    # the higher order 4 bytes is the epoch
    (zxid_epoch,) = struct.unpack('>i', zxid_bytes[0:4])
    # the lower order 4 bytes is the count
    (zxid_count,) = struct.unpack('>i', zxid_bytes[4:8])

    return zxid_epoch, zxid_count

def parse_admin_wchc(text):
    """
    Parser zookeeper admin command `wchp`
    
    wchp - watches by session
        
        
      0x15dc0117fd6633a
            /clusterstate.json
            /clusterprops.json
            /aliases.json
      0x15dc0117fd66384
            /clusterstate.json
            /clusterprops.json
            /aliases.json
      0x15dc0117fd670fe
            /clusterstate.json
            /clusterprops.json
            /aliases.json
      0x15dc0117fd631b9
            /clusterstate.json
            /collections/efc-jobs-suggest/leader_elect/shard1/election/242534181306977427-core_node3-n_0000000111
            /overseer_elect/election/242534181306977427-10.51.65.74:8983_solr-n_0000121138
            /collections/efc-profiles-col/state.json
            /collections/efc-profiles-col/leader_elect/shard1/election/242534181306977427-core_node10-n_0000000443
            /collections/jsm-efc-jobs-col/state.json
            /collections/efc-jobs-suggest/state.json
            /collections/efc-jobsearch-col/state.json
            /configs/efc-jobs-suggest-2017-07-26T21:38:56.326374
            /security.json
            /collections/efc-jobsearch-col/leader_elect/shard1/election/242534181306977427-core_node9-n_0000000246
            /configs/efc-profiles-match-2017-03-06T22:33:54.325668
            /configs/efc-jobs-2017-07-27T15:52:13.401112
            /clusterprops.json
            /collections/jsm-efc-jobs-col/leader_elect/shard1/election/242534181306977427-core_node6-n_0000001274
            /configs/jsm-efc-jobs-2017-08-14T22:58:17.350259
            /collections/efc-profiles-match-col/leader_elect/shard1/election/242534181306977427-core_node10-n_0000000245
            /configs/efc-profiles-2017-06-08T15:34:56.672279
            /aliases.json
            /collections/efc-profiles-match-col/state.json
            /configs/jsm-efc-jobs-2017-08-16T23:43:42.259417
      0x15dc0117fd631b6
            /clusterstate.json
            /collections/efc-profiles-col/state.json
            /collections/jsm-efc-jobs-col/state.json
            /collections/efc-jobs-suggest/state.json
            /collections/efc-jobsearch-col/state.json
            /configs/efc-jobs-suggest-2017-07-26T21:38:56.326374
            /security.json
            /configs/efc-profiles-match-2017-03-06T22:33:54.325668
            /configs/efc-jobs-2017-07-27T15:52:13.401112
            /clusterprops.json
            /configs/jsm-efc-jobs-2017-08-14T22:58:17.350259
            /configs/efc-profiles-2017-06-08T15:34:56.672279
            /aliases.json
            /collections/efc-profiles-match-col/state.json
            /configs/jsm-efc-jobs-2017-08-16T23:43:42.259417
    """
    data = defaultdict(list)
    ZNODE_IDENT = '/'
    SESSION_IDENT = '0x'
    session = None
    path = None
    for line in text.split("\n"):
    
        line = line.strip()
        
        if not line:
            continue 
        
        if line.startswith(ZNODE_IDENT):
            path = line
        elif line.startswith(SESSION_IDENT):
            session = hex_or_none(line)
        else:
            continue

        if not all((session, path)):
            continue
            
        data[session].append(path)
        
    return data
    
def parse_admin_wchp(text):
    """
    Parser zookeeper admin command `wchp`
    
    wchp - watches by node name.
    
    Example::
    
        /collections/efc-profiles-col/leader_elect/shard1/election/98445948263739830-core_node8-n_0000000442
            0x35da78d8ab14c93
        /collections/jsm-efc-jobs-col/leader_elect/shard1/election/98445948263739830-core_node4-n_0000001273
            0x35da78d8ab14c93
        /security.json
            0x35da78d8ab14c93
        /overseer_elect/election/98445948263739830-10.51.64.251:8983_solr-n_0000121137
            0x35da78d8ab14c93
        /configs/efc-profiles-match-2017-03-06T22:33:54.325668
            0x35da78d8ab14c93
        /configs/jsm-efc-jobs-2017-08-14T22:58:17.350259
            0x35da78d8ab14c93
        /aliases.json
            0x25d9a46df6374da
            0x15d9a46de66261f
            0x35da78d8ab16651
            0x35da78d8ab16ad3
            0x35da78d8ab1664f
            0x35da78d8ab16a05
            0x25d9a46df63262a
            0x35da78d8ab1178b
            0x35da78d8ab14c93
        /collections/efc-profiles-match-col/state.json
            0x35da78d8ab14c93
        /clusterstate.json
            0x25d9a46df6374da
            0x15d9a46de66261f
            0x35da78d8ab16651
            0x35da78d8ab16ad3
            0x35da78d8ab1664f
            0x35da78d8ab16a05
            0x25d9a46df63262a
            0x35da78d8ab1178b
            0x35da78d8ab14c93
        /collections/efc-profiles-col/state.json
            0x35da78d8ab14c93
        /collections/jsm-efc-jobs-col/state.json
            0x35da78d8ab14c93
        /collections/efc-jobs-suggest/state.json
            0x35da78d8ab14c93
        /collections/efc-jobsearch-col/state.json
            0x35da78d8ab14c93
        /configs/efc-jobs-suggest-2017-07-26T21:38:56.326374
            0x35da78d8ab14c93
        /collections/efc-jobs-suggest/leader_elect/shard1/election/98445948263739830-core_node1-n_0000000110
            0x35da78d8ab14c93
        /configs/efc-jobs-2017-07-27T15:52:13.401112
            0x35da78d8ab14c93
        /clusterprops.json
            0x25d9a46df6374da
            0x15d9a46de66261f
            0x35da78d8ab16651
            0x35da78d8ab16ad3
            0x35da78d8ab1664f
            0x35da78d8ab16a05
            0x25d9a46df63262a
            0x35da78d8ab1178b
            0x35da78d8ab14c93
        /collections/efc-jobsearch-col/leader_elect/shard1/election/98445948263739830-core_node7-n_0000000245
            0x35da78d8ab14c93
        /collections/efc-profiles-match-col/leader_elect/shard1/election/98445948263739830-core_node8-n_0000000244
            0x35da78d8ab14c93
        /configs/efc-profiles-2017-06-08T15:34:56.672279
            0x35da78d8ab14c93
        /configs/jsm-efc-jobs-2017-08-16T23:43:42.259417
            0x35da78d8ab14c93
    """
    data = defaultdict(list)
    ZNODE_IDENT = '/'
    SESSION_IDENT = '0x'
    session = None
    path = None
    for line in text.splitlines():
    
        line = line.strip()
        
        if not line:
            continue 
        
        if line.startswith(ZNODE_IDENT):
            path = line
        elif line.startswith(SESSION_IDENT):
            session = hex_or_none(line)
        else:
            continue

        if not all((session, path)):
            continue
            
        data[path].append(session)
        
    return data
        
def parse_admin_cons(text):
    """
    Parse zookeeper admin command 'cons' output into a data structure.

    `cons` returns connection information for a particular server.

    sid is the session id
    lop is the last operation performed by the client
    est is the time the session was originally established
    to is the negotiated client timeout
    lcxid is the last client transaction id
    lzxid is the last zxid (-1 is used for pings)
    lresp is the last time that the server responded to a client request
    llat is the latency of the latest operation
    minlat/avglat/maxlat are the min/avg/max latency for the session in milliseconds

    Example::

      /10.100.200.113:25037[0](queued=0,recved=1,sent=0)
      /10.17.72.197:44830[1](queued=0,recved=230457,sent=230457,sid=0x35b643799ab346b,lop=GETD,est=1496775835190,to=15000,lcxid=0x38435,lzxid=0x1500074205,lresp=1496785171003,llat=0,minlat=0,avglat=0,maxlat=15)
      /10.17.73.20:55374[1](queued=0,recved=9758,sent=9758,sid=0x35b643799ab3458,lop=GETD,est=1496774945164,to=15000,lcxid=0x223c,lzxid=0x1500074204,lresp=1496785169077,llat=0,minlat=0,avglat=0,maxlat=3)
      /10.17.73.130:37298[1](queued=0,recved=205948,sent=205948,sid=0x35b643799ab345d,lop=GETD,est=1496775142238,to=15000,lcxid=0x32469,lzxid=0x1500074205,lresp=1496785171187,llat=0,minlat=0,avglat=0,maxlat=6)
      /10.17.73.20:57459[1](queued=0,recved=2065,sent=2065,sid=0x35b643799ab3467,lop=PING,est=1496775601756,to=15000,lcxid=0xaf,lzxid=0xffffffffffffffff,lresp=1496785168136,llat=0,minlat=0,avglat=0,maxlat=3)
      /10.100.200.113:25036[1](queued=0,recved=1,sent=1,sid=0x35b643799ab350a,lop=SESS,est=1496785170946,to=15000,lcxid=0x0,lzxid=0x1500074205,lresp=1496785170949,llat=2,minlat=0,avglat=2,maxlat=2)
      /10.17.72.21:46963[1](queued=0,recved=1929,sent=1929,sid=0x35b643799ab3465,lop=PING,est=1496775600025,to=15000,lcxid=0x11,lzxid=0xffffffffffffffff,lresp=1496785167415,llat=0,minlat=0,avglat=0,maxlat=3)
      /10.50.66.190:40744[1](queued=0,recved=1752,sent=1762,sid=0x35b643799ab344e,lop=PING,est=149677443
      
      
    Outputs::
    
        [{'avglat': 0,
          'client': ['10.51.65.171', '41322'],
          'connections': 1,
          'est': 1496782363613L,
          'lcxid': 165956,
          'llat': 1,
          'lop': 'PING',
          'lresp': 1496799079512L,
          'lzxid': (67, 1684),
          'maxlat': 5,
          'minlat': 0,
          'queued': 0,
          'recved': 8251,
          'sent': 8251,
          'sid': 170105861950612956L,
          'to': 15000,
          'ueued': '0'},
          {...},
          {...}]
    """
    data = []
    for line in text.splitlines():
        entry = {
            'client': None,
            'connections': 0,
            'queued': 0,
            'recved': 0,
            'sent': 0,
            'sid': None,
            'lop': None,
            'est': None,
            'to': None,
            'lcxid': None,
            'lzxid': None,
            'lresp': None,
            'llat': None,
            'minlat': None,
            'avglat': None,
            'maxlat': None
        }

        line = line.strip()

        if not line.startswith('/'):
            continue

        addr, other = line.split('[', 1)
        addr, port = addr.split(':')

        entry['client'] = [addr[1:], port]

        cons_count, other = other.split(']', 1)
        entry['connections'] = int_or_none(cons_count)

        if not other.startswith('('):
            raise ValueError("unexpected format... expected start char: '(' got: %s" % other) #XXX
            continue

        if other.endswith(')'):
            other = other[1:-1]
        else:
            other = other[1:]

        sess_vals = other[1:].split(',')
        for val in sess_vals:
            if '=' not in val:
                continue

            key, strval = val.strip().split('=')
            entry[key] = strval

        entry['queued'] = int_or_none(entry['queued'])
        entry['recved'] = int_or_none(entry['recved'])
        entry['sent']   = int_or_none(entry['sent'])
        entry['sid']    = hex_or_none(entry['sid'])
        entry['est']    = int_or_none(entry['est'])
        entry['to']     = int_or_none(entry['to'])
        entry['lcxid']  = hex_or_none(entry['lcxid'])
        entry['lzxid']  = parse_zxid(entry['lzxid'])
        entry['lresp']  = int_or_none(entry['lresp'])
        entry['llat']   = int_or_none(entry['llat'])
        entry['minlat'] = int_or_none(entry['minlat'])
        entry['avglat'] = int_or_none(entry['avglat'])
        entry['maxlat'] = int_or_none(entry['maxlat'])

        data.append(entry)

    return data


def parse_admin_dump(text):
    """
    Example Input::

          SessionTracker dump:
          Session Sets (13):
          0 expire at Tue Jun 06 22:51:20 UTC 2017:
          0 expire at Tue Jun 06 22:51:24 UTC 2017:
          1 expire at Tue Jun 06 22:51:28 UTC 2017:
                0x15c7ea7f00e002c
          6 expire at Tue Jun 06 22:51:32 UTC 2017:
                0x15c7ea7f00e002f
                0x15c7ea7f00e0028
                0x15c7ea7f00e0123
                0x35b643799ab3467
                0x25b643799ff348f
                0x15c7ea7f00e0034
          19 expire at Tue Jun 06 22:51:36 UTC 2017:
                0x25b643799ff3479
                0x25b643799ff3492
                0x25b643799ff3487
                0x15c7ea7f00e002d
                0x25b643799ff3573
                0x15c7ea7f00e003a
                0x35b643799ab3553
                0x25b643799ff3574
                0x35b643799ab345d
                0x35b643799ab3465
                0x35b643799ab346b
                0x25b643799ff3482
                0x35b643799ab345a
                0x25b643799ff3575
                0x25b643799ff347f
                0x15c7ea7f00e002a
                0x25b643799ff34a2
                0x15c7ea7f00e0031
                0x35b643799ab3458
          0 expire at Tue Jun 06 22:51:40 UTC 2017:
          0 expire at Tue Jun 06 22:51:44 UTC 2017:
          0 expire at Tue Jun 06 22:51:48 UTC 2017:
          0 expire at Tue Jun 06 22:51:52 UTC 2017:
          0 expire at Tue Jun 06 22:51:56 UTC 2017:
          1 expire at Tue Jun 06 22:52:00 UTC 2017:
                0x15c7ea7f00e000e
          1 expire at Tue Jun 06 22:52:04 UTC 2017:
                0x25b643799ff3464
          1 expire at Tue Jun 06 22:52:08 UTC 2017:
                0x35b643799ab344e
          ephemeral nodes dump:
          Sessions with Ephemerals (3):
          0x25b643799ff3464:
                /collections/efc-jobsearch-col/leader_elect/shard1/election/169839600926143588-core_node1-n_0000000181
                /live_nodes/10.50.65.133:8983_solr
                /overseer_

    Outputs::
    
        {'ephemerals': {170105861950612946L: ['/collections/efc-profiles-col/leaders/shard1/leader',
                                              '/collections/efc-profiles-match-col/leader_elect/shard1/election/170105861950612946-core_node10-n_0000000230',
                                              '/live_nodes/10.51.64.201:8983_solr',
                                              '/collections/jsm-efc-jobs-col/leaders/shard1/leader',
                                              '/overseer_elect/leader',
                                              '/collections/jsm-efc-jobs-col/leader_elect/shard1/election/170105861950612946-core_node6-n_0000000152',
                                              '/collections/efc-profiles-col/leader_elect/shard1/election/170105861950612946-core_node10-n_0000000205',
                                              '/collections/efc-jobsearch-col/leaders/shard1/leader',
                                              '/collections/efc-jobs-suggest/leaders/shard1/leader',
                                              '/collections/efc-jobs-suggest/leader_elect/shard1/election/170105861950612946-core_node3-n_0000000094',
                                              '/collections/efc-jobsearch-col/leader_elect/shard1/election/170105861950612946-core_node12-n_0000000225',
                                              '/overseer_elect/election/170105861950612946-10.51.64.201:8983_solr-n_0000000528',
                                              '/collections/efc-profiles-match-col/leaders/shard1/leader'],
                        170105861950612956L: ['/collections/efc-profiles-match-col/leader_elect/shard1/election/170105861950612956-core_node9-n_0000000232',
                                              '/collections/efc-profiles-col/leader_elect/shard1/election/170105861950612956-core_node9-n_0000000207',
                                              '/overseer_elect/election/170105861950612956-10.51.65.171:8983_solr-n_0000000530',
                                              '/live_nodes/10.51.65.171:8983_solr',
                                              '/collections/efc-jobsearch-col/leader_elect/shard1/election/170105861950612956-core_node11-n_0000000227',
                                              '/collections/jsm-efc-jobs-col/leader_elect/shard1/election/170105861950612956-core_node5-n_0000000154',
                                              '/collections/efc-jobs-suggest/leader_elect/shard1/election/170105861950612956-core_node2-n_0000000095']},
         'sessions': [{'expires': datetime.datetime(2017, 6, 7, 1, 31, 32, tzinfo=tzutc()),
                       'session': 170150722986836265L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 34, tzinfo=tzutc()),
                       'session': 242208317088530708L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 34, tzinfo=tzutc()),
                       'session': 170150722986836267L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 34, tzinfo=tzutc()),
                       'session': 98048267905335296L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 34, tzinfo=tzutc()),
                       'session': 170150722986836266L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 36, tzinfo=tzutc()),
                       'session': 170105861950612956L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 36, tzinfo=tzutc()),
                       'session': 170105861950613222L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 36, tzinfo=tzutc()),
                       'session': 242163455995094763L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 38, tzinfo=tzutc()),
                       'session': 170105861950613284L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 38, tzinfo=tzutc()),
                       'session': 98048267905345512L},
                      {'expires': datetime.datetime(2017, 6, 7, 1, 31, 40, tzinfo=tzutc()),
                       'session': 170105861950612946L}]}
                
    """
    SESSIONS = 'sessions'
    EPHEMERALS = 'ephemerals'
    mode = None
    bucket = None
    data = {
        SESSIONS: [],
        EPHEMERALS: {},
    }
    for line in text.splitlines():
        if 'sessiontracker dump' in line.lower():
            mode = SESSIONS
        elif 'ephemeral nodes dump' in line.lower():
            mode = EPHEMERALS
        elif 'expire at' in line:
            count, date = line.split('expire at')
            if date.endswith(':'):
                date = date[:-1]
            date =  parser.parse(date)
            bucket = date
        elif '0x' in line:
            if line.endswith(':'):
                line = line[:-1]

            session = hex_or_none(line)

            if mode == SESSIONS:
                data[SESSIONS].append({'session': session, 'expires': bucket})
            elif mode == EPHEMERALS:
                bucket = session

        elif mode == EPHEMERALS and line.strip().startswith('/'):
            if bucket not in data[EPHEMERALS]:
                data[EPHEMERALS][bucket] = []

            data[EPHEMERALS][bucket].append(line.strip())

    return data