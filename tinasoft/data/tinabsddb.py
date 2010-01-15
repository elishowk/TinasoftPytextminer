# -*- coding: utf-8 -*-
from tinasoft.data import Handler
import bsddb

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
        self._db = bsddb.btopen(path)
        self.cursor = None
        self.commit = None
        self.prefix = {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'Ngram::',
            'AssocCorpus':'AssocCorpus::',
            'AssocDocument':'AssocDocument::',
            'AssocNGramDocument':'AssocDocument::',
            'AssocNGramCorpus':'AssocNGramCorpus::'
        }


    #def execute(self, *args):
    #    return self.cursor().execute(*args)

    #def executemany(self, *args):
    #    return self.cursor().executemany(*args)

    def encode( self, obj ):
        return self.serialize(obj)

    def decode( self, data ):
        return self.deserialize( data )

    # TODO asynchron calls
    #def setCallback(self, cb):
    #    self.callback = cb

    def saferead( self, key ):
        """returns a sqlite3 cursor or catches exceptions"""
        try:
            return self._db.get(key)
        except DBError, e1:
            print e1,req
            return None

    def safereadall(self):
        raise NotImplemented

    def safewrite( self, key, obj ):
        try:
            if self._db.exists(key) is False:
                self._db.put(key, self.encode(obj))
                return True
            else:
                return False
        except DBError, e1:
            print e1,req
            return False

    def safewritemany( self, iter ):
        for key, obj in iter.iteritems():
            self.safewrite( key, obj )

    def dump(self, filename, compress=None):
        raise NotImplemented

# Engine frontend
class Engine(Backend):

    """
    bsddb Engine
    """

    def insertCorpora(self, id, obj):
        self.safewrite( self.prefix['Corpora']+id, obj )

    def insertCorpus(self, id, period_start, period_end, obj):
        # id, period_start, period_end, blob
        self.safewrite( self.prefix['Corpus']+id, obj )

    def insertmanyCorpus(self, iter):
        # id, period_start, period_end, blob
        for key, obj in iter.iteritems():
            self.insertCorpus( self.prefix['Corpus']+key, obj )

    def insertDocument(self, id, datestamp, obj):
        # id, datestamp, blob
        self.safewrite( self.prefix['Document']+id, obj )

    def insertmanyDocument(self, iter):
        # id, datestamp, blob
         for key, obj in iter.iteritems():
            self.insertDocument( self.prefix['Document']+key, obj )

    def insertNGram(self, id, obj):
        self.safewrite( self.prefix['NGram']+id, obj )

    def insertmanyNGram(self, iter):
         for key, obj in iter.iteritems():
            self.insertNGram( self.prefix['NGram']+key, obj )

    def insertAssoc(self, assocname, tuple ):
        raise NotImplemented

    def insertmanyAssoc(self, iter, assocname):
        raise NotImplemented

    def deletemanyAssoc( self, iter, assocname ):
        raise NotImplemented

    def insertAssocCorpus(self, corpusID, corporaID ):
        key = self.prefix['AssocCorpus']+corporaID
        self.safewrite( key, corpusID )

    def insertAssocDocument(self, docID, corpusID ):
        key = self.prefix['AssocDocument']+corpusID
        self.safewrite( key, docID )

    def insertAssocNGramDocument(self, ngramID, docID, occs ):
        key = self.prefix['AssocNGramDocument']+docID
        self.safewrite( key, [ ngramID, occs ] )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs ):
        key = self.prefix['AssocNGramCorpus']+corpID
        self.safewrite( key, [corpID, occs] )

    def insertmanyAssocNGramDocument( self, iter ):
        raise NotImplemented

    def insertmanyAssocNGramCorpus( self, iter ):
        raise NotImplemented

    def deletemanyAssocNGramDocument( self, iter ):
        raise NotImplemented

    def deletemanyAssocNGramCorpus( self, iter ):
        raise NotImplemented

    #def cleanAssocNGramDocument( self, corpusNum ):
    #    req = self.cleanAssocNGramDocumentStmt()
    #    arg = [corpusNum]
    #    return self.safewrite(req, arg)

    def loadCorpora(self, id ):
        raise NotImplemented

    def loadCorpus(self, id ):
        raise NotImplemented

    def loadDocument(self, id ):
        raise NotImplemented

    def loadNGram(self, id ):
        raise NotImplemented

    def fetchCorpusNGram( self, corpusid ):
        raise NotImplemented

    def fetchCorpusNGramID( self, corpusid ):
        raise NotImplemented

    def fetchDocumentNGram( self, documentid ):
        raise NotImplemented

    def fetchDocumentNGramID( self, documentid ):
        raise NotImplemented

    def fetchCorpusDocumentID( self, corpusid ):
        raise NotImplemented

    def dropTables( self ):
        raise NotImplemented


    def createTables( self ):
        raise NotImplemented

    def clear( self ):
        raise NotImplemented

