#coding:utf-8

from protocol import BTNetService, BTCommand
import itertools
import time
import getopt
import sys
import os
import seed

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

def buildSeedFile(targetFile, tracker, seedFileName) :
    if os.path.exists(seedFileName) :
        print 'seed file : %s already exist.' % (seedFileName,)
        exit()
    if not os.path.exists(targetFile) :
        print 'target file : %s not found.' % (targetFile,)
        exit()
    if os.path.isdir(targetFile) :
        print 'target file : %s is directory.' % (targetFile,)
        exit()
    
    seedFile = seed.createSeedFile(targetFile, tracker, seedFileName)
    print seedFile

def publishFile(todir, seedFile) :
    pass

def downloadFile(todir, seedFile) :
    pass

def usage() :
    print '''Usage: client.py [options...] <seed file>
 -h --help\t\t\tPrint this usage
 -b --build file\t\tBuild seed file
 -t --tracker ip:port\t\tSpecify tracker addr
 -d --todir path\t\tSpecify target dir
 
 Example :
 Create a seed file :
     client.py -b targetFile -t trackerAddr seedFileName.bt
 Download file :
     client.py seedFileName.bt
     client.py -d targetDir seedFileName.bt
 Publish file :
     client.py -s -d targetDir seedFileName.bt
 '''

def main(argv) :
    try :
        opts, args = getopt.getopt(argv, 'hb:t:d:p', ['help', 'build=', 'tracker=', 'todir=', 'publish'])
    except getopt.GetoptError :
        usage()
        exit(1)
    build = None
    tracker = None
    todir = None
    seedFile = None
    publish = False
    for opt, arg, in opts :
        if opt in ('-h', '--help') :
            usage()
        elif opt in ('-b', '--build') :
            build = arg
        elif opt in ('-t', '--tracker') :
            tracker = arg
        elif opt in ('-d', '--todir') :
            todir = arg
        elif opt in ('-p', '--publish') :
            publish = True
    if len(args) > 0 :
        seedFile = args[0]
    
    print build, tracker, todir, seedFile
    if seedFile == None :
        usage()
        exit(1)
    
    if build != None :
        if tracker == None :
            usage()
            exit(1)
        buildSeedFile(build, tracker, seedFile)
        exit()

    if publish :
        if todir == None :
            usage()
            exit(1)
        publishFile(todir, seedFile)
        exit()

    downloadFile(todir, seedFile)

if __name__ == '__main__' :
    main(sys.argv[1:])
