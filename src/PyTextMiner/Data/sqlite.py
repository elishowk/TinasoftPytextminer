# -*- coding: utf-8 -*-

from PyTextMiner import Data
import sqlite3

# LOW-LEVEL BACKEND 
class SQLiteBackend (Data.Importer):
    def __init__(self, path, **opts):
 
        self.path = path
        self.locale = self.get_property(opts, 'locale', 'en_US.UTF-8')
        self.format = self.get_property(opts, 'format', 'json')

        self.lang,self.encoding = self.locale.split('.')

        self._db = sqlite3.connect(path)
        self._cursor = self._db.cursor()
        self.execute = self._cursor.execute
        self.commit = self._db.commit
        self.tables = []

        if self.format == 'yaml':
            import yaml
            self.encode = yaml.dump
            self.decode = yaml.load
        elif self.format == 'json' or self.format == 'jsonpickle':
            import jsonpickle
            self.encode = jsonpickle.encode
            self.decode = jsonpickle.decode
        else:
            try:
                import cpickle as pickle
            except:
                import pickle
            self.encode = pickle.dumps
            self.decode = pickle.loads
          
    def setCallback(self, cb):
        self.callback = cb
          
    def getTable(self, obj):
        cName = obj.__class__.__name__
        if cName in self.tables:
            return cName
        self.tables.append(cName)
        try:
            if cName.startswith('Assoc'):
                self.execute('''create table '''+cName+''' (id1 VARCHAR, id2 VARCHAR, PRIMARY KEY (id1, id2))''')
            else:
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, blob text)''')

            self.commit()
        except sqlite3.OperationalError, exc:
            print "ERROR:",exc
            # table already exists
            pass
        return cName

    def insert(self, id, obj):
        req = 'insert into ' + self.getTable(obj) + ' values (?, ?)'
        try:
            self.execute(req,(id,self.encode(obj)))
            self.commit()
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return False
        return True
        
    def insertAssoc(self, assoc):
        i1, i2 = assoc
        req = 'insert into ' + self.getTable(assoc) + ' values (?, ?)'
        try:
            self.execute(req, assoc)
            self.commit()
        except sqlite3.IntegrityError, e1:
            print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "ASSOC Operational error:",e2,req
            return False
        return True
        
    def update(self, id, obj):
        self.execute("""update """ + self.getTable(obj) + """ SET (blob = '%s') WHERE  (id LIKE '%s')"""%(self.encode(obj), id))
        self.commit()
        return True
        
    def fetch(self, clss, id=None):
        results = []
        if id is None:
            results = self.execute("""select * from """ +clss.__name__)
        else:
            results = self.execute("""select * from """ + clss.__name__ + """ WHERE  (id == '%s') LIMIT 1"""%id)
        if id is None:
            return [self.decode(blob) for i, blob in results]
        else:
            i, blob = results
            return self.decode(blob)


# APPLICATION LAYER  
class Assoc (tuple):
    pass
class AssocCorpus (Assoc):
    pass
class AssocDocument (Assoc):
    pass
class AssocNGram (Assoc):
    pass
    
class Exporter (SQLiteBackend):

    def storeCorpora(self,  corpora, id ):
        return self.insert( id, corpora )
        
    def storeCorpus(self, corpus, id ):
        return self.insert( id, corpus )

    def storeDocument(self, document, id ):
        return self.insert( id, document )

    def storeNGram(self, ngram, id ):
        return self.insert( id, ngram )

    def storeAssocCorpus(self, corpusID, corporaID ):
        assoc = AssocCorpus((corpusID, corporaID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocDocument(self, docID, corpusID ):
        assoc = AssocDocument((docID, corpusID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocNGram(self, ngramID, docID ):
        assoc = AssocNGram((ngramID, docID))
        return self.insertAssoc( assoc )


    def loadCorpora(self, id ):
        return self.backend.fetch( id )
        
    def loadCorpus(self, id ):
        return self.backend.fetch( id )
        
    def loadDocument(self, id ):
        return self.backend.fetch( id )
        
    def loadNGram(self, id ):
        return self.backend.fetch( id )
        
    def loadAssocCorpus(self, id ):
        return self.backend.fetch( id )
             
    def loadAssocDocument(self, id ):
        return self.backend.fetch( id )
             
    def loadAssocNGram(self, id ):
        return self.backend.fetch( id )
   
