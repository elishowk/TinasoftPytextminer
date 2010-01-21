# -*- coding: utf-8 -*-
from tinasoft.data import Handler
from bsddb3 import db
import re

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
            self._db = db.DB()
            #self._db.set_flags(db.DB_DUPSORT)
            self._db.set_flags(db.DB_AUTO_COMMIT)
            self._db.open(path, None, db.DB_BTREE, db.DB_CREATE)
            self.cursor = self._db.cursor
            self.dbenv = db.DBEnv()
            #self.commit = None
        except db.DBError, dbe:
            print dbe
        self.prefix = {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'NGram::',
        }

    def __del__(self):
        self._db.sync()

    def encode( self, obj ):
        return self.serialize(obj)

    def decode( self, data ):
        return self.deserialize(data)

    def saferead( self, key ):
        """returns a db entry"""
        #try:
        return self._db.get(key)
        #except db.DBError, e1:
        #    print e1
        #    return None

    def safereadall( self, smallestkey=None ):
        """returns a cursor, optional smallest key"""
        cur = self.cursor()
        if smallestkey is not None:
            cur.set_range( smallestkey )
        return cur

    def safewrite( self, key, obj ):
        #try:
            #txn = self.dbenv.txn_begin()
            #res = self._db.put(key, self.encode(obj), None, db.DB_OVERWRITE_DUP)
        res = self._db.put(key, self.encode(obj))
            #txn.commit()
        return res
        #except db.DBError, e:
        #    print "DBError :", e
            #txn.discard()
        #    return False

    def safewritemany( self, iter ):
        for key, obj in iter.iteritems():
            self.safewrite( key, obj )

    def safedelete( self, key ):
        self._db.delete( key )

    def dump(self, filename, compress=None):
        raise NotImplemented

class Engine(Backend):

    """
    bsddb Engine
    """

    def insertCorpora(self, obj, id=None):
        if id is None:
            id = obj.id
        self.safewrite( self.prefix['Corpora']+id, obj )

    def insertCorpus(self, obj, id=None, period_start=None, period_end=None):
        if id is None:
            id = obj.id
        # id, period_start, period_end, blob
        self.safewrite( self.prefix['Corpus']+id, obj )

    def insertmanyCorpus(self, iter):
        # id, period_start, period_end, blob
        for obj in iter:
            self.insertCorpus( obj )

    def insertDocument(self, obj, id=None, datestamp=None):
        if id is None:
            id = obj.id
        # id, datestamp, blob
        self.safewrite( self.prefix['Document']+id, obj )

    def insertmanyDocument(self, iter):
        # id, datestamp, blob
         for obj in iter:
            self.insertDocument( obj )

    def insertNGram(self, obj, id=None):
        if id is None:
            id = obj.id
        self.safewrite( self.prefix['NGram']+id, obj )

    def insertmanyNGram(self, iter):
         for obj in iter:
            self.insertNGram( obj )

    def insertAssoc(self, load, id, target, targetID, occs, insert):
        obj = load( id )
        if obj is None:
            return
        if target not in obj['content']:
            obj['content'][target]={}
        if targetID in obj['content'][target]:
            obj['content'][target][targetID]+=1
        else:
            obj['content'][target][targetID]=occs
        insert( obj, str(obj['id']) )

    def insertmanyAssoc(self, iter, assocname):
        raise NotImplemented

    def deletemanyAssoc( self, iter, assocname ):
        raise NotImplemented

    def insertAssocCorpus(self, corpusID, corporaID, occs=1 ):
        self.insertAssoc( self.loadCorpora, corporaID, 'Corpus', corpusID, occs, self.insertCorpora )
        self.insertAssoc( self.loadCorpus, corpusID, 'Corpora', corporaID, occs, self.insertCorpus )

    def insertAssocDocument(self, docID, corpusID, occs=1 ):
        self.insertAssoc( self.loadCorpus, corpusID, 'Document', docID, occs, self.insertCorpus )
        self.insertAssoc( self.loadDocument, docID, 'Corpus', corpusID, occs, self.insertDocument )

    def insertAssocNGramDocument(self, ngramID, docID, occs ):
        self.insertAssoc( self.loadDocument, docID, 'NGram', ngramID, occs, self.insertDocument )
        self.insertAssoc( self.loadNGram, ngramID, 'Document', docID, occs, self.insertNGram )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs ):
        self.insertAssoc( self.loadCorpus, corpID, 'NGram', ngramID, occs, self.insertCorpus )
        self.insertAssoc( self.loadNGram, ngramID, 'Corpus', corpID, occs, self.insertNGram )

    def insertmanyAssocNGramDocument( self, iter ):
        for tup in iter:
            self.insertAssocNGramDocument( tup[0], tup[1], tup[2] )

    def insertmanyAssocNGramCorpus( self, iter ):
        for tup in iter:
            self.insertAssocNGramCorpus( tup[0], tup[1], tup[2] )

    def deletemanyAssocNGramDocument( self, iter ):
        raise NotImplemented

    def deletemanyAssocNGramCorpus( self, iter ):
        raise NotImplemented

    def load(self, id, target):
        read = self.saferead( self.prefix[target]+id )
        print read
        if read is not None:
            return self.decode(read[1])
        else:
            return None

    def loadCorpora(self, id ):
        self.load(id, 'Corpora')

    def loadCorpus(self, id ):
        self.load(id, 'Corpus')

    def loadDocument(self, id ):
        self.load(id, 'Document')

    def loadNGram(self, id ):
        self.load(id, 'NGram')

    def fetchCorpusNGram( self, corpusid ):
        raise NotImplemented

    def fetchCorpusNGramID( self, corpusid ):
        corpus = self.loadCorpus( corpusid )
        return corpus['content']['NGram']

    def fetchDocumentNGram( self, documentid ):
        raise NotImplemented

    def fetchDocumentNGramID( self, documentid ):
        doc = self.loadDocument( documentid )
        return corpus['content']['NGram']

    def fetchCorpusDocumentID( self, corpusid ):
        c = self.loadCorpus( corpusid )
        return corpus['content']['Document']

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

