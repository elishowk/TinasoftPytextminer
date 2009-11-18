# -*- coding: utf-8 -*-

import sqlite3
import jsonpickle
    
class SQLiteBackend (object):
    def __init__(self, path='/tmp/test-sqlite-backend'):
        self.tables = []
        self._db = sqlite3.connect(path)
        self._cursor = self.conn.cursor()
        self.execute = self._cursor.execute
        self.commit = self._db.commit
            
    def _addClass(self, cName):
        if cName in self.tables:
            return False
        self.tables.append(cName)
        self.execute('''create table '''+cName+''' (id INTEGER PRIMARY KEY, blob text)''')
        self.commit()

    def insert(self, obj):
        self.execute("""insert into """ + obj.__class__.__name__ + """ values (%s, "%s")"""%(id(obj),jsonpickle.encode(obj)))
        self.commit()
        return id(obj)
        
    def update(self, obj):
        self.execute("""update """ + obj.__class__.__name__ + """ SET (blob = "%s") WHERE  (id LIKE %s)"""%(jsonpickle.encode(obj), id(obj)))
        self.commit()
        return id(obj)
           
        
class Importer (Data.Importer):
    def __init__(self, path, locale='en_US:UTF-8'):
        self.locale =  locale
        self.lang,self.encoding = self.locale.split(':')
        file = codecs.open(path, "rU", self.encoding)
        self.documents = Importer._load_documents(file, self.locale)
        self.corpus = Corpus( name=path, documents=self.documents )

class AssocCorpus (tuple):
    pass
class AssocDocument (tuple):
    pass
class AssocNGram (tuple):
    pass
    
class Exporter (Data.Importer):
    def __init__(self, path, locale='en_US:UTF-8'):
        self.locale =  locale
        self.lang,self.encoding = self.locale.split(':')
        self.backend = SQLiteBackend(path)
      
    def storeCorpora(self, corpora ):
        return self.backend.insert( corpora )
        
    def storeCorpus(self, corpus ):
        return sself.backend.insert( corpus )

    def storeDocument(self, document ):
        return self.backend.insert( document )

    def storeNGram(self, ngram ):
        return self.backend.insert( ngram )

    def storeAssocCorpus(self, corpusID, corporaID ):
        return self.backend.insert( AssocCorpus(corpusID, corporaID) )
        
    def storeAssocDocument(self, docID, corpusID ):
        return self.backend.insert( AssocDocument(docID, corpusID) )
        
    def storeAssocNGram(self, ngramID, docID ):
        return self.backend.insert( AssocNGram(ngramID, docID) ) 
        
        
