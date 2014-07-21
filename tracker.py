#coding=utf-8

import network as nw


class TrackerHandler(nw.IoHandler) :
    def __init__(self, tracker) :
        self.tracker = tracker

    def connected(self, ioSession) :
        pass

    def closed(self, ioSession) :
        pass

    def exceptionCaught(self, ioSession, e) :
        ioSession.close()

    def msgRecv(self, ioSession, msg) :
        pass

    def msgSent(self, ioSession, msg) :
        pass

class Tracker(object) :
    def __init__(self, port) :
        self.port = port
        self.ioService = nw.IoService()
        self.ioService.setHandler(TrackerHandler(self))

    def run(self) :
        self.ioService.listen(self.port)
        self.ioService.run()


if __name__ == '__main__' :
    tracker = Tracker(1988)
    tracker.run()

