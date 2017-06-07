import socket
import time
def netcat(hostname, port, content, timeout=5):
    """
    Operate similary to netcat command in linux (nc).
    """
    # SOCK_DGRAM = UDP 
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hostname, int(port)))
    sock.settimeout(timeout)
 
    # send the request
    try:
        sock.sendall(content)
    except socket.timeout, e:
        raise IOError("connect timeout: %s calling [%s] on [%s]" % (e, content, hostname))
    except socket.error, e: # subclass of IOError
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
            response += msg
        except socket.timeout, e:
            raise IOError("%s calling [%s] on [%s]" % (e, content, hostname))
        except socket.error, e:
            raise IOError("%s calling [%s] on [%s]" % (e, content, hostname))
 
        if len(msg) == 0:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            break
            
    return response