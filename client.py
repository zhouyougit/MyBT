#coding:utf-8

from protocol import BTNetService, BTCommand
import itertools
import time

def respCallback(response) :
    print response.result

rc = itertools.count()

def timerTask(service) :
    print 'timerTask fire'
    service.sendRequest(('localhost', 1988), 'text', respCallback, now = time.time(), rid = rc.next())
    service.addTimerEvent(timerTask, service, 3000)


class BTClient(object) :
    def __init__(self) :
        self.service = BTNetService()
        self.service.addTimerEvent(timerTask, self.service, 3000)

    def run(self) :
        self.service.run()

if __name__ == '__main__' :
    client = BTClient()
    client.run()
