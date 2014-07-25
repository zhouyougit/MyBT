import sqlite3
import time
import seed
import os
import hashlib

class DownloadStat(object) :
    def __init__(self, targetDir, seedFile, publish = False) :
        self.targetDir = targetDir
        self.seed = seedFile
        self.publish = publish

        fileName = self.seed.getProp(seed.KEY_FILE_NAME)
        self.statName = self.targetDir + os.path.sep + fileName + '.stat'
        self.targetName = self.targetDir + os.path.sep + fileName

        if publish :
            self.__publish()
        else :
            self.__download()

    def createStatFile(self) :
        self.openStatFile()
        self.conn.execute('''
        CREATE TABLE piece (
            idx INTEGER PRIMARY KEY,
            stat INTEGER DEFAULT 0
        )''')
        pieceCount = self.seed.getProp(seed.KEY_PIECE_COUNT)
        self.conn.executemany('insert into piece(idx) values (?)', [(i,) for i in range(1, int(pieceCount)+1)])

    def openStatFile(self) :
        self.conn = sqlite3.connect(self.statName)
        self.conn.isolation_level = None

    def checkStat(self) :
        if not os.path.exists(self.targetName):
            raise StatException, 'targetFile %s is not exist.' % (self.targetName,)

        fileSize = int(self.seed.getProp(seed.KEY_LENGTH))
        if fileSize != os.path.getsize(self.targetName) :
            raise StatException, 'targetFile %s size incorrect. seed file record is %s, but actually file size is %s' % (self.targetName,fileSize, os.path.getsize(self.targetName))

        if not os.path.exists(self.statName) :
            self.createStatFile()
        
        checksum = self.seed.getChecksum()
        finished = []
        unfinished = []
        fileChecksum = hashlib.sha1()
        csIter = iter(checksum)
        pieceSize = int(self.seed.getProp(seed.KEY_PIECE_SIZE))
        idx = 0

        with file(self.targetName) as inFile :
            while True :
                subFile = inFile.read(pieceSize)
                if len(subFile) == 0 :
                    break
                idx += 1
                fileChecksum.update(subFile)
                pieceChecksum = hashlib.sha1()
                pieceChecksum.update(subFile)
                subCs = pieceChecksum.hexdigest()[:20]
                if subCs == csIter.next() :
                    finished.append(idx)
                else :
                    unfinished.append(idx)
        
        if not unfinished and fileChecksum.hexdigest() != self.seed.getProp(seed.KEY_CHECKSUM) :
            raise StatException, 'targetFile %s checksum incorrect.' % (self.targetName,)

        self.conn.executemany('update piece set stat = 1 where idx = ?', [(i,) for i in finished])
        self.conn.executemany('update piece set stat = 0 where idx = ?', [(i,) for i in unfinished])

        return True

    def getPercent(self) :
        pieceCount = int(self.seed.getProp(seed.KEY_PIECE_COUNT))
        cur = self.conn.execute('select count(1) from piece where stat = 1')
        finishedCount = cur.fetchone()[0]
        return finishedCount * 100 / pieceCount

    def getStatStr(self) :
        cur = self.conn.execute('select stat from piece order by idx')
        data = cur.fetchall()
        result = ''
        for idx in range(0, len(data), 8) :
            code = 0
            for pos, stat in enumerate(data[idx : idx + 8]) :
                code |= (stat[0] << (7 - pos))
            result += chr(code)
        return result

    def __publish(self) :
        if not os.path.exists(self.targetName):
            raise StatException, 'targetFile %s is not exist.' % (self.targetName,)

        if os.path.exists(self.statName) :
            self.openStatFile()
        else :
            self.createStatFile()

        self.checkStat()

        if self.getPercent() != 100 :
            raise StatException, 'targetFile %s incomplate.' % (self.targetName,)

    def __download(self) :
        if os.path.exists(self.statName) :
            self.openStatFile()
        else :
            self.createStatFile()


class StatException(Exception) :
    pass
