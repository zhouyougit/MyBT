#coding=utf-8

from protocol import BTNetService, BTCommand

class TextCommand(BTCommand) :
    def process(self, request, response) :
        print 'received request', request.param
        response.result = 'received :' + str(request.param)

class BTTracker(object) :
    def __init__(self, port) :
        self.port = port
        self.service = BTNetService()
        self.service.addCmd('text', TextCommand())

    def run(self) :
        self.service.listen(self.port)
        self.service.run()


if __name__ == '__main__' :
    tracker = BTTracker(1988)
    tracker.run()

