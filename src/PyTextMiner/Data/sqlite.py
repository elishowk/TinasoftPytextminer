# -*- coding: utf-8 -*-

from PyTextMiner import Corpus, Document, Corpora, NGram, Data
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
          
    def getTable(self, clss):
        cName = clss.__name__
        print "cName:",cName
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
            # table already exists
            pass
        return cName

    def insert(self, id, obj):
        print "obj class:",obj.__class__
        req = 'insert into ' + self.getTable(obj.__class__) + ' values (?, ?)'
        try:
            self.execute(req,(self.encode(id),self.encode(obj)))
            self.commit()
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return False
        return True
        
    def insertAssoc(self, assoc):
        i1,i2 = assoc
        i1,i2 = self.encode(i1), self.encode(i2)
        req = 'insert into ' + self.getTable(assoc.__class__) + ' values (?, ?)'
        try:
            self.execute(req, (i1,i2))
            self.commit()
        except sqlite3.IntegrityError, e1:
            print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "ASSOC Operational error:",e2,req
            return False
        return True
        
    def update(self, id, obj):
        req = 'update '+ self.getTable(obj.__class__) +' SET (blob = ?) WHERE (id LIKE ?)'
        self.execute(req, (self.encode(obj), self.encode(id)))
        self.commit()
        return True
      
    def fetch_all(self, clss):
        req = ('select * from '+self.getTable(clss))
        reply = self.execute(req)
        results = []
        for i, blob in reply:
            results.append((self.decode(i), self.decode(blob)))
        return tuple (results )
        
    def fetch_one(self, clss, id):

        req = 'select * from '+ self.getTable(clss) + ' WHERE (id LIKE ?)'#%self.encode(id)
        results = self.execute(req, [self.encode(id)])   

        i, blob = None, None
       
        for res in results:
            i, blob = res
            i = self.decode(i)
            blob = self.decode(blob)
            break

        return blob
        
    def clean(self):
        """remove unused tables (empty)"""
        raise NotImplemented
        
    def clear(self):
        """clear all the tables"""
        print "self.tables:",self.tables
        for table in ['NGram',
                      'Document',
                      'Corpus',
                      'Corpora',
                      'AssocCorpus',
                      'AssocDocument',
                      'AssocNGram']:
            try:
                self.execute('DROP TABLE '+table) 
            except:
                pass  


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

    def storeCorpora(self,  id, corpora ):
        return self.insert( id, corpora )
        
    def storeCorpus(self, id, corpus ):
        return self.insert( id, corpus )

    def storeDocument(self, id, document ):
        print "doc",
        return self.insert( id, document )

    def storeNGram(self, id, ngram ):
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
        return self.fetch_one( Corpora, id )
        
    def loadCorpus(self, id ):
        return self.fetch_one( Corpus, id )
        
    def loadDocument(self, id ):
        return self.fetch_one( Document, id )
        
    def loadNGram(self, id ):
        return self.fetch_one( NGram, id )
        
        
    def loadAllAssocCorpus(self):
        return self.fetch_all( AssocCorpus )
             
    def loadAllAssocDocument(self):
        return self.fetch_all( AssocDocument )
             
    def loadAllAssocNGram(self):
        return self.fetch_all( AssocNGram )
   
