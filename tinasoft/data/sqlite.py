# -*- coding: utf-8 -*-
from sqlapi import Api
from tinasoft.data import Handler

try:
    import sqlite3
except Exception, e:
    print "Unable to import sqlite3"
    pass

# LOW-LEVEL BACKEND
class Backend(Handler):

    options = {
        'locale'     : 'en_US.UTF-8',
        'format'     : 'jsonpickle',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None
    }

    def __init__(self, path, **opts):

        # sql api
        #self.api = sqlapi.Api()
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')
        self._db = sqlite3.connect(path)
        self.cursor = self._db.cursor
        # deprecated : using try/except automaticcaly commit and rollbak
        self.commit = self._db.commit

    def execute(self, *args):
        return self.cursor().execute(*args)

    def executemany(self, *args):
        return self.cursor().executemany(*args)

    def encode( self, data ):
        # sqlite3.Binary() may be faster than
        # the python buffer() conversion of an str
        return sqlite3.Binary( self.serialize(data) )

    def decode( self, buf ):
        # sqlite3 blob returns a python buffer type
        return self.deserialize( str(buf) )

    # TODO asynchron calls
    #def setCallback(self, cb):
    #    self.callback = cb

    def saferead( self, req, tuple ):
        """returns a sqlite3 cursor or catches exceptions"""
        try:
            with self._db:
                return self._db.execute(req, tuple)
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return None
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return None

    def safereadone( self, req, tuple ):
        """returns the first result or None"""
        read = self.saferead( req, tuple )
        if read is not None:
            return read.fetchone()
        else:
            return None

    # deprecated : avoid reading a SELECT result at once
    def safereadall( self, req, tuple ):
        read = self.saferead( req, tuple )
        return read.fetchall()

    def safewrite( self, req, tuple ):
        try:
            with self._db:
                self._db.execute(req, tuple)
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return False
        return True

    def safewritemany( self, req, iter ):
        try:
            with self._db:
                self._db.executemany(req, iter)
        except sqlite3.Error, e1:
            print "sqlite3 error ",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return False
        return True

    def dump(self, filename, compress=None):
        try:
            f = open(filename, 'w')
            for line in self._db.iterdump():
                f.write('%s\n' % line)
            f.close()
        except:
            print "Couldn't open output file to dump the SQL"
            return None

        if compress is 'gzip' or compress is 'gz' or compress is 'zip':
            try:
                import gzip
                f = open(filename,'r')
                content.read()
                f.close()
                f = gzip.open(filename, 'wb')
                f.write(content)
                f.close()
            except Exception, exc:
                print "Couldn't save gzipped version:",exc
                pass

# Engine frontend
class Engine(Backend, Api):

    """
    For large tables (Assoc and NGram),
    must use the 'storemany' version
    """
    def insertCorpora(self, id, obj):
        req = self.insertCorporaStmt()
        tuple = (id,self.encode(obj))
        self.safewrite( req, tuple )

    def insertCorpus(self, id, period_start, period_end, corpus):
        # id, period_start, period_end, blob
        req = self.insertCorpusStmt()
        tuple = ( id, period_start, period_end, self.encode( corpus ) )
        self.safewrite( req, tuple )

    def insertmanyCorpus(self, iter):
        # id, period_start, period_end, blob
        req = self.insertCorpusStmt()
        self.safewritemany( req, iter )

    def insertDocument(self, id, datestamp, obj):
        # id, datestamp, blob
        req = self.insertDocumentStmt()
        tuple = ( id, datestamp, self.encode(obj) )
        self.safewrite( req, tuple )

    def insertmanyDocument(self, iter):
        # id, datestamp, blob
        req = self.insertDocumentStmt()
        self.safewritemany( req, iter )

    def insertNGram(self, id, obj):
        req = self.insertNGramStmt()
        tuple = (id,self.encode(obj))
        self.safewrite( req, tuple )

    def insertmanyNGram(self, iter):
        req = self.insertNGramStmt()
        self.safewritemany( req, iter )

    def insertAssoc(self, assocname, tuple ):
        req = self.insertAssocStmt( assocname )
        self.safewrite( req, tuple )

    def insertmanyAssoc(self, iter, assocname):
        req = self.insertAssocStmt( assocname )
        self.safewritemany( req, iter )

    def deletemanyAssoc( self, iter, assocname ):
        req = self.deleteAssocStmt( assocname )
        self.safewritemany( req, iter )

    def insertAssocCorpus(self, corpusID, corporaID ):
        assoc = (corpusID, corporaID)
        return self.insertAssoc( 'AssocCorpus', assoc )

    def insertAssocDocument(self, docID, corpusID ):
        assoc = (docID, corpusID)
        return self.insertAssoc( 'AssocDocument', assoc )

    def insertAssocNGramDocument(self, ngramID, docID, occs ):
        assoc = (ngramID, docID, occs)
        return self.insertAssoc( 'AssocNGramDocument', assoc )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs ):
        assoc = (ngramID, corpID, occs)
        return self.insertAssoc( 'AssocNGramCorpus', assoc )

    def insertmanyAssocNGramDocument( self, iter ):
        return self.insertmanyAssoc( iter, 'AssocNGramDocument' )

    def insertmanyAssocNGramCorpus( self, iter ):
        return self.insertmanyAssoc( iter, 'AssocNGramCorpus' )

    def deletemanyAssocNGramDocument( self, iter ):
        return self.deletemanyAssoc( iter, 'AssocNGramDocument' )

    def deletemanyAssocNGramCorpus( self, iter ):
        return self.deletemanyAssoc( iter, 'AssocNGramCorpus' )

    #def cleanAssocNGramDocument( self, corpusNum ):
    #    req = self.cleanAssocNGramDocumentStmt()
    #    arg = [corpusNum]
    #    return self.safewrite(req, arg)

    def loadCorpora(self, id ):
        req = self.loadCorporaStmt()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res

    def loadCorpus(self, id ):
        req = self.loadCorpusStmt()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], res[2], self.decode( res[3] ) ]
        else:
            return res

    def loadDocument(self, id ):
        req = self.loadDocumentStmt()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], self.decode( res[2] ) ]
        else:
            return res

    def loadNGram(self, id ):
        req = self.loadNGramStmt()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res

    def fetchCorpusNGram( self, corpusid ):
        req = self.fetchCorpusNGramStmt()
        reply = self.saferead(req, [corpusid])
        return reply
        #results = []
        #if reply is not None:
        #    reply = reply.fetchall()
        #    for blob in reply:
        #        results.append( ( id, self.deserialize(blob[0]) ) )
        #return results

    def fetchCorpusNGramID( self, corpusid ):
        req = self.fetchCorpusNGramIDStmt()
        reply = self.saferead(req, [corpusid])
        return reply
        #results = []
        #if reply is not None:
        #    reply = reply.fetchall()
        #    for id in reply:
        #        results.append( self.deserialize(id[0]) )
        #return results

    def fetchDocumentNGram( self, documentid ):
        req = self.fetchDocumentNGramStmt()
        reply = self.saferead(req, [documentid])
        return reply
        #results = []
        #if reply is not None:
        #    reply = reply.fetchall()
        #    for blob in reply:
        #        results.append( self.deserialize(blob[0]))
        #return results

    def fetchDocumentNGramID( self, documentid ):
        req = self.fetchDocumentNGramIDStmt()
        reply = self.saferead(req, [documentid])
        return reply
        #results = []
        #if reply is not None:
        #    reply = reply.fetchall()
        #    for id in reply:
        #        results.append( id[0] )
        #return results

    def fetchCorpusDocumentID( self, corpusid ):
        req = self.fetchCorpusDocumentIDStmt()
        reply = self.saferead(req, [corpusid])
        return reply
        #results = []
        #if reply is not None:
        #    reply = reply.fetchall()
        #    for id in reply:
        #        results.append( id[0] )
        #return results

    def dropTables( self ):
        try:
            with self._db:
                for drop in self.dropTablesStmt():
                    self.execute( drop )
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return False
        return True


    def createTables( self ):
        for create in self.createTablesStmt():
            self.execute( create )
        self.commit()

    def clear( self ):
        self.dropTables()
        self.createTables()
