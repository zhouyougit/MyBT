#coding=utf-8

import struct
import hessian2
import network as nw

MAGIC_NUM = 'BT'
HEADER_LENGTH = 8
BT_VERSION = 1
FLAG_REQ = 0x01

'''
Header Format :
0        8        16       24       32
+--------+--------+--------+--------+
|   Magic Number  | Status |  Flag  |
+--------+--------+--------+--------+
|            Data Length            |
+--------+--------+--------+--------+

Status Format :
0 - 2 : Protocol Version (1 - 7)
3 - 7 : Status Code (0 - 31)

Flag Format :
0 :
1 :
'''

class BTRequest(object) :
    def __init__(self) :
        pass
    
class BTResponse(object) :
    def __init__(self) :
        pass

class BTCommand(object) :
    def process(self, request, response) :
        pass

class BTNetHandler(nw.IoHandler) :
    def __init__(self, service) :
        self.service = service

    def connected(self, ioSession) :
        if 'request' in ioSession.attr :
            request = ioSession.attr['request']
            del ioSession.attr['request']
            addrKey = ioSession.attr['addrKey']
            self.service.sessionMap[addrKey] = ioSession
            ioSession.write(encodeRequest(request))
            ioSession.read()
        else :
            ioSession.read()

    def closed(self, ioSession) :
        print 'session', ioSession.remoteAddr(), ioSession.localAddr(), 'closed'
        if 'addrKey' in ioSession.attr and ioSession.attr['addrKey'] in self.service.sessionMap :
            del self.service.sessionMap[ioSession.attr['addrKey']]

    def exceptionCaught(self, ioSession, e) :
        ioSession.close()
    
    def __readData(self, ioSession, obj, data) :
        if len(data) < obj.dataLength :
            ioSession.attr['header'] = obj
            ioSession.attr['dataBuf'] = data
            return
        obj.data = data
        if isinstance(obj, BTRequest) :
            self.service.processRequest(ioSession, obj)
        elif 'callback' in ioSession.attr :
            decodeResponse(obj)
            ioSession.attr['callback'](obj)

    def __readHeader(self, ioSession, msg) :
        if len(msg) < HEADER_LENGTH :
            ioSession.attr['headerBuf'] = msg
        else :
            obj = decodeHeader(msg[:HEADER_LENGTH])
            data = msg[HEADER_LENGTH:]
            self.__readData(ioSession, obj, data)

    def msgRecv(self, ioSession, msg) :
        if 'headerBuf' in ioSession.attr :
            msg = ioSession.attr['headerBuf'] + msg
            del ioSession.attr['headerBuf']
            self.__readHeader(ioSession, msg)
        elif 'dataBuf' in ioSession.attr :
            data = ioSession.attr['dataBuf'] + msg
            del ioSession.attr['dataBuf']
            header = ioSession.attr['header']
            del ioSession.attr['header']
            self.__readData(ioSession, header, data)
        else :
            self.__readHeader(ioSession, msg)

    def msgSent(self, ioSession, msg) :
        pass

class BTNetService(object) :
    def __init__(self) :
        self.ioService = nw.IoService()
        self.ioService.setHandler(BTNetHandler(self))
        self.sessionMap = {}
        self.commands = {}

    def addCmd(self, name, command) :
        self.commands[name] = command

    def listen(self, port, host = '') :
        self.ioService.listen(port, host)
    
    def sendRequest(self, addr, cmd, callback, **param) :
        request = self.createRequest(cmd, **param)
        addrKey = str(addr[0]) + ':' + str(addr[1])
        if addrKey in self.sessionMap :
            session = self.sessionMap[addrKey]
            session.attr['callback'] = callback
            session.write(encodeRequest(request))
        else :
            self.ioService.connect(addr, request = request, callback = callback, addrKey = addrKey)
    
    def processRequest(self, session, request) :
        decodeRequest(request)
        if request.cmd in self.commands :
            cmd = self.commands[request.cmd]
            response = self.createResponse()
            cmd.process(request, response)
            session.write(encodeResponse(response))
    
    def createRequest(self, cmd, **param) :
        request = BTRequest()
        request.cmd = cmd
        request.param = param
        request.status = 0
        request.flag = FLAG_REQ
        return request

    def createResponse(self) :
        response = BTResponse()
        response.status = 0
        response.flag = 0
        return response

    def run(self) :
        self.ioService.run()

    def addTimerEvent(self, callback, data, expires) :
        self.ioService.addTimerEvent(callback, data, expires)

##encode and decode
def encodeRequest(request) :
    header = MAGIC_NUM
    status = BT_VERSION << 5
    header += chr(status)
    header += chr(request.flag | FLAG_REQ)
    out = hessian2.Hessian2Output()
    out.writeObject(request.cmd)
    out.writeObject(request.param)
    data = out.getByteString()
    dataLength = len(data)
    header += struct.pack('>I', dataLength)
    return header + data

def decodeRequest(request) :
    hInput = hessian2.Hessian2Input(request.data)
    request.cmd = hInput.readObject()
    request.param = hInput.readObject()

def encodeResponse(response) :
    header = MAGIC_NUM
    status = (BT_VERSION << 5) + (response.status & 0x1F)
    header += chr(status)
    header += chr(response.flag)
    out = hessian2.Hessian2Output()
    if response.status == 0 :
        out.writeObject(response.result)
    else :
        out.writeObject(response.error)
    data = out.getByteString()
    dataLength = len(data)
    header += struct.pack('>I', dataLength)
    return header + data

def decodeResponse(response) :
    hInput = hessian2.Hessian2Input(response.data)
    if response.status == 0 :
        response.result = hInput.readObject()
    else :
        response.error = hInput.readObject()

def decodeHeader(header) :
    if header[:2] != MAGIC_NUM :
        return None
    
    status = ord(header[2])
    version = status >> 5
    status = status & 0x1F
    flag = ord(header[3])
    dataLength = struct.unpack('>I', header[4:8])[0]
    if flag & FLAG_REQ :
        obj = BTRequest()
    else :
        obj = BTResponse()
    obj.status = status
    obj.version = version
    obj.flag = flag
    obj.dataLength = dataLength
    return obj

