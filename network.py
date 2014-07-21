#coding=utf-8

import heapq
import itertools
import time
import socket
import select
import threading
import errno
import thread

try :
    import fcntl
except ImportError :
    fcntl = None

class BaseReactor(object) :
    MAX_READ_SIZE = 4096
    def __init__(self) :
        pass

    def addReader(self, reader) :
        pass

    def addWriter(self, writer) :
        pass

    def addListener(self, listener) :
        pass

    def removeReader(self, reader) :
        pass
    
    def removeWriter(self, writer) :
        pass

    def removeListener(self, listener) :
        pass

    def loop(self, timeout) :
        pass

    def fireException(self, sessions) :
        for session in sessions :
            session.ioService.handler.exceptionCaught(session, None)

    def fireRead(self, sessions) :
        for session in sessions :
            msg = []
            try :
                while True :
                    submsg = session.skt.recv(BaseReactor.MAX_READ_SIZE)
                    msg.append(submsg)
                    if len(submsg) < BaseReactor.MAX_READ_SIZE :
                        break
            except socket.error, e :
                break
            if len(msg) == 1 :
                allMsg = msg[0]
            else :
                allMsg = ''.join(msg)

            session.ioService.handler.msgRecv(session, allMsg)

    def fireWrite(self, sessions) :
        for session in sessions :
            if session.isConnecting :
                session.isConnecting = False
                session.suspendWrite()
                err = session.skt.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err != 0 :
                    exception = socket.error(err, errno.errorcode[err])
                    session.ioService.handler.exceptionCaught(session, exception)
                else :
                    session.ioService.handler.connected(session)
                continue
            wq = session.writeQueue
            if wq :
                msg = wq[0]
                if session.writeOffset :
                    sent = session.skt.send(msg[session.writeOffset:])
                else :
                    sent = session.skt.send(msg)
                if sent < (len(msg) - session.writeOffset) :
                    session.writeOffset += sent
                else :
                    del wq[0]
                    session.writeOffset = 0
                    session.ioService.handler.msgSent(session, msg)
            if not wq :
                session.suspendWrite()

    def fireConnect(self, sessions) :
        for session in sessions :
            skt, addr  = session.skt.accept()
            skt.setblocking(False)
            newSession = session.ioService.createIoSession(skt)
            session.ioService.handler.connected(newSession)

class SelectReactor(BaseReactor) :
    def __init__(self) :
        self._readers = set()
        self._writers = set()
    
    def addReader(self, reader) :
        if reader not in self._readers :
            self._readers.add(reader)

    def addWriter(self, writer) :
        if writer not in self._writers :
            self._writers.add(writer)

    def removeReader(self, reader) :
        if reader in self._readers :
            self._readers.remove(reader)
    
    def removeWriter(self, writer) :
        if writer in self._writers :
            self._writers.remove(writer)

    def loop(self, timeout) :
        r, w, e = select.select(self._readers, self._writers, set.union(self._readers, self._writers), timeout)

        if e :
            self.fireException(e)

        if r :
            rrs = []
            rls = []
            for rl in r :
                if rl.isListener :
                    rls.append(rl)
                else :
                    rrs.append(rl)
            if rrs :
                self.fireRead(rrs)
            if rls :
                self.fireConnect(rls)

        if w :
            self.fireWrite(w)

def getReactor() :
    return SelectReactor

class IoSession(object) :
    def __init__(self, ioService, skt) :
        self.ioService = ioService
        self.sid = ioService.sidCounter.next()
        self.attr = {}
        self.skt = skt
        self.isListener = False
        self.isConnecting = False
        self.writeQueue = []
        self.writeOffset = 0
        self.isWriting = False
        self.isReading = False

    def read(self) :
        self.resumeRead()

    def write(self, msg) :
        self.writeQueue.append(msg)
        self.resumeWrite()

    def suspendRead(self) :
        if not self.isReading :
            return
        self.isReading = False
        self.ioService.reactor.removeReader(self)

    def suspendWrite(self) :
        if not self.isWriting :
            return
        self.isWriting = False
        self.ioService.reactor.removeWriter(self)

    def resumeRead(self) :
        if self.isReading :
            return
        self.isReading = True
        self.ioService.reactor.addReader(self)

    def resumeWrite(self) :
        if self.isWriting :
            return
        self.isWriting = True
        self.ioService.reactor.addWriter(self)

    def close(self) :
        if self.isReading :
            self.ioService.reactor.removeReader(self)
        if self.isWriting :
            self.ioService.reactor.removeWriter(self)
        if self.isListener :
            self.ioService.reactor.removeListener(self)
        self.skt.close()
        self.attr = {}
        self.isListener = False
        self.isConnecting = False
        self.writeQueue = []
        self.writeOffset = 0
        self.isWriting = False
        self.isReading = False
        self.ioService.freeIoSession(self)

    def remoteAddr(self) :
        return self.skt.getpeername()

    def localAddr(self) :
        return self.skt.getsockname()

    def fileno(self) :
        return self.skt.fileno()

class IoHandler(object) :
    def connected(self, ioSession) :
        pass

    def closed(self, ioSession) :
        pass

    def exceptionCaught(self, ioSession, e) :
        print e
        ioSession.close()

    def msgRecv(self, ioSession, msg) :
        pass

    def msgSent(self, ioSession, msg) :
        pass

class TimerEventQueue(object) :
    def __init__(self) :
        self.timerQueue = []
        self.timerMap = {}
        self.timerCount = itertools.count()

    def getFireEvents(self) :
        if not self.timerQueue :
            return []
        now = int(time.time() * 1000)
        result = []
        while self.timerQueue and self.timerQueue[0][0] <= now :
            expires, taskId, event = heapq.heappop(self.timerQueue)
            result.append(event)
        return result

    def getNextExpires(self) :
        if not self.timerQueue :
            return 0
        return self.timerQueue[0][0]

    def addTimerEvent(self, event, expires) :
        taskId = self.timerCount.next()
        task = [expires, taskId, event]
        heapq.heappush(self.timerQueue, task)
        self.timerMap[taskId] = task
        return taskId

    def removeTimerEvent(self, taskId) :
        if taskId not in self.timerMap :
            return
        self.timerMap[taskId][-1] = None

    def removeAllTimerEvent(self) :
        self.timerMap = {}
        self.timerQueue = []

class IoService(object) :
    DEFAULT_TIMEOUT = 10
    MAX_FREE_SESSION = 10
    def __init__(self, addressFamily = socket.AF_INET, socketType = socket.SOCK_STREAM) :
        self.handler = None
        self.timerEventQueue = TimerEventQueue()
        self.reactor = getReactor()()
        self.running = False
        self.needAdd = False
        self.addressFamily = addressFamily
        self.socketType = socketType
        self.sessions = set()
        self.freeSessions = []
        self.sidCounter = itertools.count()
        self.addCond = threading.Condition()
        self.addLock = threading.Lock()
        self.addEvent = None

    def setHandler(self, channelHandler) :
        self.handler = channelHandler

    def listen(self, port, host = '', backlog = 100) :
        skt = self._createSocket()
        addr = (host, port)
        skt.bind(addr)
        skt.listen(backlog)
        session = self.createIoSession(skt)
        session.isListener = True
        session.read()

    def addTimerEvent(self, callback, data, expires) :
        self.timerEventQueue.addTimerEvent((callback, data), expires)

    def removeTimerEvent(self, taskId) :
        self.timerEventQueue.removeTimerEvent(taskId)

    def removeAllTimerEvent(self) :
        self.timerEventQueue.removeAllTimerEvent()

    def run(self) :
        self.running = True
        while self.running :
            while not self.needAdd :
                nextExpires = self.timerEventQueue.getNextExpires()
                now = int(time.time() * 1000 )
                if nextExpires == 0 or (nextExpires - now) > IoService.DEFAULT_TIMEOUT:
                    timeout = float(IoService.DEFAULT_TIMEOUT) / 1000
                elif nextExpires <= now :
                    timeout = 0
                else :
                    timeout = float(nextExpires - now) / 1000 
                self.reactor.loop(timeout)

                timerEvents = self.timerEventQueue.getFireEvents()
                if timerEvents :
                    for event in timerEvents :
                        if not callable(event[0]) :
                            continue
                        event[0](event[1])
            self.addCond.acquire()
            self.needAdd = False
            if self.addEvent :
                self.addTimerEvent(*(self.addEvent))
            self.addCond.notify()
            self.addCond.release()

    def stop(self) :
        self.running = False
        self.needAdd = True
    
    def _createSocket(self) :
        s = socket.socket(self.addressFamily, self.socketType)
        s.setblocking(False)
        if fcntl :
            flags = fcntl.fcntl(s, fcntl.F_GETFD)
            flags = flags | fcntl.FD_CLOEXEC
            fcntl.fcntl(s, fcntl.F_SETFD, flags)
        return s

    def createIoSession(self, skt) :
        if self.freeSessions :
            session = self.freeSessions.pop()
            session.sid = ioService.sidCounter.next()
            session.skt = skt
        else :
            session = IoSession(self, skt)
        return session
    
    def freeIoSession(self, session) :
        if len(self.freeSessions) < IoService.MAX_FREE_SESSION :
            self.freeSessions.append(session)
    
    def _safeAddTimerEvent(self, callback, data, expires) :
        self.addLock.acquire()
        self.addCond.acquire()
        self.addEvent = (callback, data, expires)
        self.needAdd = True
        self.addCond.wait()
        self.addCond.release()
        self.addLock.release()
        
    def safeConnect(self, addr, **attr) :
        def createConnect(data) :
            self.connect(addr, **attr)
        self._safeAddTimerEvent(createConnect, None, 0)
    
    def connect(self, addr, **argv) :
        skt = self._createSocket()
        err = skt.connect_ex(addr)
        session = self.createIoSession(skt)
        session.isConnecting = True
        session.attr.update(attr)
        if err not in (0, errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, errno.EISCONN) :
            exception = socket.error(err, errno.errorcode[err])
            session.ioService.handler.exceptionCaught(session, exception)
            return
        session.resumeWrite()

if __name__ == '__main__' :
    class MyHandler(IoHandler) :
        def connected(self, ioSession) :
            if 'cmd' in ioSession.attr :
                ioSession.write('I am coming~~')
            ioSession.read()
        def msgRecv(self, ioSession, msg) :
            if msg.startswith('set') :
                cmd, delayTime, delayMsg = msg.split()
                def callback(data) :
                    print 'delay callback', data
                ioSession.ioService.addTimerEvent(callback, delayMsg, int(time.time() * 1000) + int(delayTime))
                ioSession.write('timer task update')
            elif msg.strip() == 'exit' :
                ioSession.write('bye~~')
                ioSession.attr['needClose'] = True
            else :
                print 'receive msg "%s" from %s' % (msg, ioSession.remoteAddr())
                ioSession.write('received ' + msg)

        def msgSent(self, ioSession, msg) :
            if 'needClose' in ioSession.attr :
                ioSession.close()

        def exceptionCaught(self, ioSession, e) :
            print e
            ioSession.close()

    ioService = IoService()
    ioService.setHandler(MyHandler())
    ioService.listen(11223)

    def task2() :
        time.sleep(3)
        ioService.safeConnect(('localhost', 11224), cmd = 'connect')
    thread.start_new_thread(task2, ())
    ioService.run()
