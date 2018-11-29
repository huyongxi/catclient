from gevent import monkey; monkey.patch_all()
import gevent
from gevent.server import StreamServer
from gevent.queue import Queue
import logging
import time
import json
import MySQLdb




logging.basicConfig(level = logging.INFO)

msgQueue = Queue(10000)


class Buffer:
    def __init__(self, session, size = 64*1024):
        self.session = session
        self.maxsize = size
        self.buffer = bytearray(size)
        self.readpos = 0
        self.writepos = 0
        self.unuse_size = self.maxsize

    def addData(self, data):
        datelen = len(data)
        if datelen > self.unuse_size:
            logging.error("%s %d Buffer Full"%self.session.address)
            return False

        #move
        if datelen > self.maxsize - self.writepos:
            self.buffer[:self.writepos - self.readpos] = self.buffer[self.readpos : self.writepos]
            self.writepos = self.writepos - self.readpos
            self.readpos = 0
        #add
        self.buffer[self.writepos : self.writepos + datelen] = data[:]
        self.writepos += datelen

        self.unuse_size = self.maxsize - self.writepos + self.readpos
        self.parse_packet()
        return True

    def parse_packet(self):
        #test
        while True:
            end = self.buffer.find(0, self.readpos, self.writepos)
            if(end == -1):#not find 0
                break
            '''
            try:
                j = json.loads(self.buffer[self.readpos : end].decode())
            except Exception as e:
                logging.error(e)
                self.readpos = end + 1
                continue
            '''
            #print(json.dumps(j, sort_keys=True, indent=4, separators=(',', ':')))
            msgQueue.put((self.session.address[0], self.buffer[self.readpos : end].decode()))
            
            self.readpos = end + 1
            #self.session.send("ACK".encode())
    
        self.unuse_size = self.maxsize - self.writepos + self.readpos



class Session:
    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.id = int(time.time()*1000000)
        self.recvbuf = Buffer(self)
        self.closed = False

    def send(self, data):
        if not self.closed:
            self.socket.sendall(data)

    def onRecv(self, data):
        self.recvbuf.addData(data)

    def close(self):
        self.closed = True
        self.socket.close()


class SessionMgr:
    session_map = {}

    @staticmethod
    def createSession(sock, addr):
        s = Session(sock, addr)
        SessionMgr.session_map[s.id] = s
        return s

    @staticmethod
    def findSessionById(id):
        return SessionMgr.session_map.get(id)

    @staticmethod
    def delSessionById(id):
        v = SessionMgr.session_map.pop(id,None)
        if v != None and not v.closed:
            v.close()



def handle(connsock, addr):
    logging.info("%s %d connected"%addr)
    ss = SessionMgr.createSession(connsock, addr)
    while True:
        try:
            data = connsock.recv(4096)
        except Exception as e:
            logging.error(e)
            break
        if not data:
            break
        else:
            ss.onRecv(data)
    SessionMgr.delSessionById(ss.id)
    logging.info("%s %d disconnected"%addr)


def save2db():
    sqlstr = "insert into app_msg (ip,json_str) values ('%s', '%s');"
    db = MySQLdb.connect(passwd="abc123",host="127.0.0.1",user="root",db="django_test")
    db.autocommit(1)
    logging.info("connect to mysql success")
    c=db.cursor()

    while True:
        r = msgQueue.get()
        c.execute(sqlstr % r)
      



if __name__ == "__main__":

    server = StreamServer(("127.0.0.1", 3000), handle)
    server.start()
    gevent.joinall([gevent.spawn(save2db),])

        