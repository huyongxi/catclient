import select
import socket
import json


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
msgTree = {}

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

        if buf[4:4+3] == b'NT1':    #binary encode
            parse_packet_binary(buf,msg_len)
        else:                       #text encode
            print(buf[4:msg_len+4].decode())
        buf = buf[msg_len+4:]


send_sock = socket.socket()
send_sock.connect(("127.0.0.1", 3000))

def parse_packet_binary(buf,msg_len):
    print("parse")
    offset = 4
    msgTree["CAT_ENCODER_VERSION"] = buf[4:4+3].decode()
    keys = ["appkey","hostname","ip","threadGroupName","threadId","threadName","messageId","parentMessageId","rootMessageId","sessionToken"]
    offset += 3

    for k in keys:
        tmp = readString(buf,offset)
        msgTree[k] = tmp[0]
        offset += tmp[1]
    
    stack = []
    while offset < msg_len+4:
        Type = buf[offset:offset+1]
        offset += 1
        if Type == b't':
            tmsg = {"child":[],"msgtype":"Transaction"}
            tmp = readLong(buf,offset)
            tmsg["timeStamp"] = tmp[0]
            offset += tmp[1]
            t_keys = ["type","name"]
            for k in t_keys:
                tmp = readString(buf,offset)
                tmsg[k] = tmp[0]
                offset += tmp[1]
            if len(stack) > 0:
                stack[-1]["child"].append(tmsg)
            else:
                msgTree["root"] = tmsg
            stack.append(tmsg)

        elif Type == b'T':
            tmsg = stack.pop()
            T_keys = ["status","data"]
            for k in T_keys:
                tmp = readString(buf,offset)
                tmsg[k] = tmp[0]
                offset += tmp[1]
            tmp = readLong(buf,offset)
            tmsg["DurationUs"] = tmp[0]
            offset += tmp[1]

        else:
            type_dict = {b'E':"Event",b'M':"Metric",b'H':"Heartbeat"}
            msg = {"msgtype":type_dict[Type]}
            tmp = readLong(buf,offset)
            msg["timeStamp"] = tmp[0]
            offset += tmp[1]
            E_keys = ["type","name","status","data"]
            for k in E_keys:
                tmp = readString(buf,offset)
                msg[k] = tmp[0]
                offset += tmp[1]

            if len(stack) > 0:
                stack[-1]["child"].append(msg)
            else:
                msgTree["root"] = msg
    if len(stack) > 0:
        print("error")
        return
    
    #print(msgTree)
    buf = bytearray(json.dumps(msgTree).encode())
    buf.append(0)
    send_sock.sendall(buf)
    msgTree.clear()




def readString(buf, offset):
    strlen,strlen_size = readLong(buf,offset)
    start = offset+strlen_size
    if strlen == 0:
        return("",strlen_size)
    return (buf[start : start+strlen].decode(),strlen + strlen_size)


def readLong(buf, offset=0, length=64):
    shift = 0
    result = 0
    i = 0
    while shift < length:
        b = buf[offset + i]
        result |= (b & 0x7f) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        i += 1
    i += 1
    return (result,i)




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
                print("recv:",fd_buf[fd],"from",sock.getpeername())
                parse_packet(fd)

        



            





