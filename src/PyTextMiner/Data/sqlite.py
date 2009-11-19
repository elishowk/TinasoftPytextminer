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
        
        if format == 'yaml':
            import yaml
            self.encode = yaml.dump
            self.decode = yaml.load
        elif format == 'json':

            self.encode = jsonpickle.encode
            self.decode = jsonpickle.decode
        else:
            try:
                import cpickle as pickle
            except:
                import pickle
            self.encode = pickle.dumps
            self.decode = pickle.loads
            
    def getTable(self, obj):
        cName = obj.__class__.__name__
        if cName in self.tables:
            return cName
        self.tables.append(cName)
        try:
            self.execute('''create table '''+cName+''' (id INTEGER, blob text)''')
            self.commit()
        except sqlite3.OperationalError:
            # table already exists
            pass
        return cName

    def insert(self, obj):
        self.execute("""insert into """ + self.getTable(obj) + """ values (%s, "%s")"""%(id(obj),self.encode(obj)))
        self.commit()
        return id(obj)
        
    def update(self, obj):
        self.execute("""update """ + self.getTable(obj) + """ SET (blob = "%s") WHERE  (id LIKE %s)"""%(self.encode(obj), id(obj)))
        self.commit()
        return id(obj)
        
    def fetch(self, clss, id=None):
        results = []
        if id is None:
            results = self.execute("""select * from """ +clss.__name__)
        else:
            results = self.execute("""select * from """ + clss.__name__ + """ WHERE  (id == %s) LIMIT 1"""%id)
        if id is None:
            return [self.decode(blob) for i, blob in results]
        else:
            i, blob = results
            return self.decode(blob)


# APPLICATION LAYER   
class AssocCorpus (tuple):
    pass
class AssocDocument (tuple):
    pass
class AssocNGram (tuple):
    pass
    
class Exporter (SQLiteBackend):

    def storeCorpora(self, corpora ):
        return self.insert( corpora )
        
    def storeCorpus(self, corpus ):
        return sself.insert( corpus )

    def storeDocument(self, document ):
        return self.insert( document )

    def storeNGram(self, ngram ):
        return self.insert( ngram )

    def storeAssocCorpus(self, corpusID, corporaID ):
        return self.insert( AssocCorpus(corpusID, corporaID) )
        
    def storeAssocDocument(self, docID, corpusID ):
        return self.insert( AssocDocument(docID, corpusID) )
        
    def storeAssocNGram(self, ngramID, docID ):
        return self.insert( AssocNGram(ngramID, docID) ) 


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
        
        
