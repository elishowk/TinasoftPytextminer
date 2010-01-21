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
            'NGram':'Ngram::',
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

    def insertAssoc(self, assocname, tuple ):
        raise NotImplemented

    def insertmanyAssoc(self, iter, assocname):
        raise NotImplemented

    def deletemanyAssoc( self, iter, assocname ):
        raise NotImplemented

    def insertAssocCorpus(self, corpusID, corporaID ):
        corpo = self.loadCorpora( corporaID )
        if 'Corpus' not in corpo['content']:
            corpo['content']['Corpus']=[]
        corpo['content']['Corpus']+=[corpusID]
        self.insertCorpora( corpo, str(corpo['id']) )

    def insertAssocDocument(self, docID, corpusID ):
        corpus = self.loadCorpus( corpusID )
        if 'Document' not in corpus['content']:
            corpus['content']['Document']=[]
        corpus['content']['Document']+=[docID]
        self.insertCorpus( corpus, str(corpus['id']) )

    def insertAssocNGramDocument(self, ngramID, docID, occs ):
        doc = self.loadDocument( docID )
        print doc
        if 'NGram' not in doc['content']:
            doc['content']['NGram']=[]
        doc['content']['NGram']+=[[ngramID, occs]]
        self.insertDocument( doc, str(doc['id']) )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs ):
        corpus = self.loadCorpus( corpID )
        if 'NGram' not in corpus['content']:
            corpus['content']['NGram']=[]
        corpus['content']['NGram']+=[[ngramID, occs]]
        self.insertCorpus( corpus, str(corpus['id']) )

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

    def loadCorpora(self, id ):
        return self.decode(self.saferead( self.prefix['Corpora']+id ))

    def loadCorpus(self, id ):
        return self.decode(self.saferead( self.prefix['Corpus']+id ))

    def loadDocument(self, id ):
        return self.decode(self.saferead( self.prefix['Document']+id ))

    def loadNGram(self, id ):
        return self.decode(self.saferead( self.prefix['NGram']+id ))

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


