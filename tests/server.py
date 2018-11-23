import select
import socket


ip_port = ("127.0.0.1", 2280)
serversock = socket.socket()

serversock.setblocking(False)
serversock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
serversock.bind(ip_port)
serversock.listen(10)

fd_sock = {}
fd_buf = {}
fd_addr = {}
epoll = select.epoll(1024)
epoll.register(serversock.fileno(), select.EPOLLIN)

def clean(fd):
    print(fd_addr[fd],"disconnect!")
    fd_sock[fd].close()
    fd_sock.pop(fd)
    fd_buf.pop(fd)
    fd_addr.pop(fd)
    epoll.unregister(fd)


def parse_packet(fd):
    buf = fd_buf[fd]
    while True:
        if len(buf) < 4:
            break
        msg_len = int.from_bytes(buf[:4], byteorder='big', signed=False)
        print("msg_len=", msg_len)
        if(msg_len + 4 > len(buf)):
            break
        print(buf[4:msg_len+4].decode())
        buf = buf[msg_len+4:]


def parse_packer_binary(fd):
    buf = fd_buf[fd]
    while True:
        if len(buf) < 4:
            break
        msg_len = int.from_bytes(buf[:4], byteorder='big', signed=False)
        print("msg_len=", msg_len)
        if(msg_len + 4 > len(buf)):
            break
        print(readString(buf[4+3:msg_len+4]))
        buf = buf[msg_len+4:]


def readString(buf, start=0, strLenSize=64):
    shift = 0
    result = 0
    i = 0
    length = strLenSize

    while shift < length:
        b = buf[start + i]
        result |= (b & 0x7f) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        i += 1
    i += 1
    return (buf[start+i : start+i+result].decode(),result + i)





while True:
    rets = epoll.poll()
    #print("events:", rets)
    for fd,event in rets:
        if fd == serversock.fileno() and event & select.EPOLLIN:
            conn,addr = serversock.accept()
            print(addr, "is connected!")
            conn.setblocking(False)
            fd_sock[conn.fileno()] = conn
            fd_buf[conn.fileno()] = b""
            fd_addr[conn.fileno()] = addr
            epoll.register(conn.fileno(), select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR | select.EPOLLRDHUP)

        elif event & (select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP):
            clean(fd)
            continue

        elif event & select.EPOLLIN:
            sock = fd_sock[fd]
            while True:
                data = b''
                data = sock.recv(4096)
                fd_buf[fd] += data
                if len(data) < 4096:
                    if len(data) == 0:
                        clean(fd)
                    break  
            if fd in fd_buf:
                //print("recv:",fd_buf[fd],"from",sock.getpeername())
                parse_packet(fd)

        



            





