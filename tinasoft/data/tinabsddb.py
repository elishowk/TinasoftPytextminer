# -*- coding: utf-8 -*-
from tinasoft.data import Handler
from bsddb3 import db

class DBInsertError(db.DBError):
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
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')
        try:
            self._dbenv = db.DBEnv()
            self._dbenv.set_lg_dir('log')
            self._db = db.DB()
            #self._db.set_flags(db.DB_DUPSORT)
            self._db.set_flags(db.DB_AUTO_COMMIT)
            self.cursor = self._db.cursor
            self._db.open(path, None, db.DB_BTREE, db.DB_CREATE)
            #self.commit = None
            #print self._db.get_flags()
        except db.DBError, dbe:
           raise Exception
        self.prefix = {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'NGram::',
            'Cooc':'Cooc::'
        }

    def __del__(self):
        try:
            self._db.close()
        except db.DBError, e:
            print "DBError :", e[1]
            raise Exception

    def encode( self, obj ):
        return self.serialize(obj)

    def decode( self, data ):
        return self.deserialize(data)

    def saferead( self, key ):
        """returns a db entry"""
        try:
            if isinstance(key, str) is False:
                key = str(key)
            return self._db.get(key)
        except db.DBNotFoundError, e1:
            print e1[1]
            return None
        except db.DBKeyEmptyError, e2:
            print e1[1]
            raise Exception

    def safereadall( self, smallestkey=None ):
        """returns a cursor, optional smallest key"""
        try:
            cur = self.cursor()
            if smallestkey is not None:
                cur.set_range( smallestkey )
            return cur
        except db.DBError, e:
            print "DBError :", e[1]
            raise Exception


    def safewrite( self, key, obj ):
        try:
            #txn = self.dbenv.txn_begin()
            if isinstance(key, str) is False:
                key = str(key)
            if self._db.exists( key ) is True:
                self._db.delete( key )
            self._db.put(key, self.encode(obj), None)
            #txn.commit()
        #return True
        except DBInsertError, dbi:
            print dbi
        except db.DBError, e:
            print "DBError :", e[1]
            raise Exception
            #txn.discard()
        #    return False

    def safewritemany( self, iter ):
        for key, obj in iter.iteritems():
            self.safewrite( key, obj )

    def safedelete( self, key ):
        try:
            if isinstance(key, str) is False:
                key = str(key)
            self._db.delete( key )
        except db.DBError, e:
            print "DBError :", e[1]
            raise Exception


    def dump(self, filename, compress=None):
        raise NotImplemented

class Engine(Backend):
    """
    bsddb Engine
    """
    def load(self, id, target):
        read = self.saferead( self.prefix[target]+id )
        if read is not None:
            return self.decode(read)
        else:
            return None

    def loadCorpora(self, id ):
        return self.load(id, 'Corpora')

    def loadCorpus(self, id ):
        return self.load(id, 'Corpus')

    def loadDocument(self, id ):
        return self.load(id, 'Document')

    def loadNGram(self, id ):
        return self.load(id, 'NGram')

    def loadCooc(self, id ):
        return self.load(id, 'Cooc')

    def insert( self, obj, target, id=None):
        if id is None:
            id = obj['id']
        self.safewrite( self.prefix[target]+id, obj )

    def insertCorpora(self, obj, id=None):
        self.insert( obj, 'Corpora', id )

    def insertCorpus(self, obj, id=None, period_start=None, period_end=None):
        self.insert( obj, 'Corpus', id )

    def insertmanyCorpus(self, iter):
        for obj in iter:
            self.insertCorpus( obj )

    def insertDocument(self, obj, id=None, datestamp=None):
        self.insert( obj, 'Document', id )

    def insertmanyDocument(self, iter):
        # id, datestamp, blob
        for obj in iter:
            self.insertDocument( obj )

    def insertNGram(self, obj, id=None):
        self.insert( obj, 'NGram', id )

    def insertmanyNGram(self, iter):
        for obj in iter:
            self.insertNGram( obj )

    def insertCooc(self, obj, id):
        self.insert( obj, 'Cooc', id )

    def insertAssoc(self, loadsource, sourcename, sourceid, insertsource, loadtarget, targetname, targetid, inserttarget, occs ):
        """adds a unique edge from source to target et and increments occs=weight"""
        sourceobj = loadsource( sourceid )
        targetobj = loadtarget( targetid )
        # returns None if one of the objects does NOT exists
        if sourceobj is None or targetobj is None:
            raise DBInsertError

        # inserts the edge in the source obj
        if targetname not in sourceobj['edges']:
            sourceobj['edges'][targetname]={}
        if targetid in sourceobj['edges'][targetname]:
            sourceobj['edges'][targetname][targetid]+=1
        else:
            sourceobj['edges'][targetname][targetid]=occs
        insertsource( sourceobj, str(sourceobj['id']) )

        # in the target obj
        if sourcename not in targetobj['edges']:
            targetobj['edges'][sourcename]={}
        if sourceid in targetobj['edges'][sourcename]:
            targetobj['edges'][sourcename][sourceid]+=1
        else:
            targetobj['edges'][sourcename][sourceid]=occs
        inserttarget( targetobj, str(targetobj['id']) )

    def insertAssocCorpus(self, corpusID, corporaID, occs=1 ):
        self.insertAssoc( self.loadCorpora, 'Corpora', corporaID,\
            self.insertCorpora, self.loadCorpus, 'Corpus', corpusID, \
            self.insertCorpus, occs )

    def insertAssocDocument(self, docID, corpusID, occs=1 ):
        self.insertAssoc( self.loadCorpus, 'Corpus', corpusID,\
            self.insertCorpus, self.loadDocument, 'Document', docID, \
            self.insertDocument, occs )

    def insertAssocNGramDocument(self, ngramID, docID, occs=1 ):
        self.insertAssoc( self.loadDocument, 'Document', docID,\
            self.insertDocument, self.loadNGram, 'NGram', ngramID, \
            self.insertNGram, occs )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs=1 ):
        self.insertAssoc( self.loadCorpus, 'Corpus', corpID,\
            self.insertCorpus, self.loadNGram, 'NGram', ngramID, \
            self.insertNGram, occs )

    def insertmanyAssocNGramDocument( self, iter ):
        for tup in iter:
            self.insertAssocNGramDocument( tup[0], tup[1], tup[2] )

    def insertmanyAssocNGramCorpus( self, iter ):
        for tup in iter:
            self.insertAssocNGramCorpus( tup[0], tup[1], tup[2] )

    def insertmanyAssoc(self, iter, assocname):
        raise NotImplemented

    def deletemanyAssoc( self, iter, assocname ):
        raise NotImplemented

    def deletemanyAssocNGramDocument( self, iter ):
        raise NotImplemented

    def deletemanyAssocNGramCorpus( self, iter ):
        raise NotImplemented

    def fetchCorpusNGram( self, corpusid ):
        raise NotImplemented

    def fetchCorpusNGramID( self, corpusid ):
        corpus = self.loadCorpus( corpusid )
        return corpus['edges']['NGram']

    def fetchDocumentNGram( self, documentid ):
        raise NotImplemented

    def fetchDocumentNGramID( self, documentid ):
        doc = self.loadDocument( documentid )
        return corpus['edges']['NGram']

    def fetchCorpusDocumentID( self, corpusid ):
        c = self.loadCorpus( corpusid )
        return corpus['edges']['Document']

    def dropTables( self ):
        raise NotImplemented

    def createTables( self ):
        raise NotImplemented

    def clear( self ):
        self._db.truncate()

    def select( self, minkey, maxkey=None ):
        cursor = self.safereadall( minkey )
        record = cursor.first()
        while record:
            if maxkey is None:
                if record[0].startswith(minkey):
                    yield ( record[0], self.decode(record[1]))
            elif record[0] < maxkey:
                yield ( record[0], self.decode(record[1]))
            else:
                return
            record = cursor.next()

