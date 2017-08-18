from __future__ import unicode_literals
import socket
import time
import six

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