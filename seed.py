#coding=utf8

import sqlite3
import os
import hashlib
import time

KEY_VERSION = 'version'
KEY_TYPE = 'type'
KEY_TRACKER = 'tracker'
KEY_CREATE_TIME = 'create_time'
KEY_COMMENT = 'comment'
KEY_CREATE_BY = 'create_by'
KEY_PIECE_SIZE = 'piece_size'
KEY_PIECE_COUNT = 'piece_count'
KEY_FILE_NAME = 'file_name'
KEY_CHECKSUM = 'checksum'
KEY_LENGTH = 'length'

class Seed(object) :
    def __init__(self, fileName) :
        self.__conn = sqlite3.connect(fileName)
        self.__conn.isolation_level = None
    
    def createTable(self) :
        self.__conn.execute('''
        CREATE TABLE prop(
            key TEXT,
            value TEXT
        );''')
        self.__conn.execute('''
        CREATE TABLE piece(
            idx INTEGER PRIMARY KEY,
            checksum TEXT
        );''')

    def setProp(self, key, value) :
        oldValue = self.getProp(key)
        if oldValue == None :
            self.__conn.execute("insert into prop values (?, ?)", (str(key), str(value)))
        else :
            self.__conn.execute("update prop set value = ? where key = ?", (str(value), str(key)))

    def getProp(self, key) :
        cur = self.__conn.execute("select value from prop where key = ?", (str(key),))
        row = cur.fetchone()
        cur.close()
        if row :
            return row[0]
        else :
            return None
    
    def setChecksum(self, checksumList) :
        self.__conn.executemany("insert into piece values (?, ?)", enumerate(checksumList, 1))
    
    def getChecksum(self, idx) :
        cur = self.__conn.execute("select checksum from piece where idx = ?", (str(idx),))
        row = cur.fetchone()
        cur.close()
        if row :
            return row[0]
        else :
            return None

    def __del__(self) :
        self.__conn.close()

    def __str__(self) :
        return '''BT seed file :
Basic infomation :
    Version     : %s
    Type        : %s
    Tracker     : %s
    Create Time : %s
    File Name   : %s
    File Size   : %s
    Piece Size  : %s
    Piece Count : %s
    Checksum    : %s''' % (self.getProp(KEY_VERSION),\
            self.getProp(KEY_TYPE),\
            self.getProp(KEY_TRACKER),\
            self.getProp(KEY_CREATE_TIME),\
            self.getProp(KEY_FILE_NAME),\
            self.getProp(KEY_LENGTH),\
            self.getProp(KEY_PIECE_SIZE),\
            self.getProp(KEY_PIECE_COUNT),\
            self.getProp(KEY_CHECKSUM))

def calculateChecksum(seed, fileName, pieceSize) :
    fileChecksum = hashlib.sha1()
    checksumList = []
    pieceCount = 0
    with file(fileName) as inFile :
        while True :
            subFile = inFile.read(pieceSize)
            if len(subFile) == 0 :
                break
            fileChecksum.update(subFile)
            pieceCount += 1
            pieceChecksum = hashlib.sha1()
            pieceChecksum.update(subFile)
            checksum = pieceChecksum.hexdigest()[:20]
            checksumList.append(checksum)
    seed.setProp(KEY_CHECKSUM, fileChecksum.hexdigest())
    seed.setProp(KEY_PIECE_COUNT, pieceCount)
    seed.setChecksum(checksumList)

def createSeedFile(targetFile, tracker, seedFile, pieceSize = 512 * 1024) :
    seed = Seed(seedFile)
    seed.createTable()
    seed.setProp(KEY_VERSION, '1.0')
    seed.setProp(KEY_TYPE, 'single')
    seed.setProp(KEY_CREATE_TIME, time.strftime('%Y-%m-%d %H:%M:%S'))
    seed.setProp(KEY_TRACKER, tracker)
    seed.setProp(KEY_PIECE_SIZE, pieceSize)
    seed.setProp(KEY_FILE_NAME, os.path.basename(targetFile))
    seed.setProp(KEY_LENGTH, os.path.getsize(targetFile))
    
    calculateChecksum(seed, targetFile, pieceSize)
    
    return seed
