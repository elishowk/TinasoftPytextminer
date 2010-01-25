# -*- coding: utf-8 -*-
from tinasoft.data import Handler
from bsddb3 import db

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
            print dbe
        self.prefix = {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'NGram::',
        }

    def __del__(self):
        self._db.close()

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
        if self._db.exists( key ) is True:
            self._db.delete( key )
        res = self._db.put(key, self.encode(obj), None)
        #res = self._db.put(key, self.encode(obj), None)
        #print res
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
    def load(self, id, target):
        read = self.saferead( self.prefix[target]+id )
        if read is not None:
            #print "found object in db : %s"%read
            return self.decode(read)
        else:
            #print "%s was NOT found"%id
            return None

    def loadCorpora(self, id ):
        return self.load(id, 'Corpora')

    def loadCorpus(self, id ):
        return self.load(id, 'Corpus')

    def loadDocument(self, id ):
        return self.load(id, 'Document')

    def loadNGram(self, id ):
        return self.load(id, 'NGram')

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

    def insertAssoc(self, loadfunction, id, target, targetID, occs, insertfunction):
        obj = loadfunction( id )
        if obj is None:
            return None
        if target not in obj['edges']:
            obj['edges'][target]={}
        if targetID in obj['edges'][target]:
            obj['edges'][target][targetID]+=1
        else:
            obj['edges'][target][targetID]=occs
        insertfunction( obj, str(obj['id']) )


    def insertAssocCorpus(self, corpusID, corporaID, occs=1 ):
        self.insertAssoc( self.loadCorpora, corporaID, 'Corpus', corpusID, occs, self.insertCorpora )
        self.insertAssoc( self.loadCorpus, corpusID, 'Corpora', corporaID, occs, self.insertCorpus )

    def insertAssocDocument(self, docID, corpusID, occs=1 ):
        self.insertAssoc( self.loadCorpus, corpusID, 'Document', docID, occs, self.insertCorpus )
        self.insertAssoc( self.loadDocument, docID, 'Corpus', corpusID, occs, self.insertDocument )

    def insertAssocNGramDocument(self, ngramID, docID, occs=1 ):
        self.insertAssoc( self.loadDocument, docID, 'NGram', ngramID, occs, self.insertDocument )
        self.insertAssoc( self.loadNGram, ngramID, 'Document', docID, occs, self.insertNGram )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs=1 ):
        self.insertAssoc( self.loadCorpus, corpID, 'NGram', ngramID, occs, self.insertCorpus )
        self.insertAssoc( self.loadNGram, ngramID, 'Corpus', corpID, occs, self.insertNGram )

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

