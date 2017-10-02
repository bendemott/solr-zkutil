"""
solrzkutil.formatter contains functionality to format various data structures or strings into human
readable content.
"""

from __future__ import print_function
from __future__ import unicode_literals

import six

def format_host(host_tuple):
    """
    Format a host tuple to a string
    """
    if isinstance(host_tuple, (list, tuple)):
        if len(host_tuple) != 2:
            raise ValueError('host_tuple has unexpeted length: %s' % host_tuple)
        
        return ':'.join([six.text_type(s) for s in host_tuple])
    elif isinstance(host_tuple, six.string_types):
        return host_tuple
    else:
        raise ValueError('host_tuple unexpected type: (%s) %s' % (type(host_tuple), host_tuple))
