# -*- coding: utf-8 -*-

from PyTextMiner import Corpus, Document, Corpora, NGram, Data
import sqlite3

# LOW-LEVEL BACKEND 
class SQLiteBackend (Data.Importer):

    options = {
        'locale'     : 'en_US.UTF-8',
        'format'     : 'pickle',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None
    }

    def __init__(self, path, **opts):
 
        self.path = path

        self.load_options(opts)
  
        self.lang,self.encoding = self.locale.split('.')

        self._db = sqlite3.connect(path)
        self.cursor = self._db.cursor
        self.commit = self._db.commit
        self.tables = []

        if self.format == 'yaml':
            import yaml
            self.serializer = yaml.dump
            self.deserializer = yaml.load
        elif self.format == 'json' or self.format == 'jsonpickle':
            import jsonpickle
            self.serializer = jsonpickle.encode
            self.deserializer = jsonpickle.decode
        else:
            try:
                import cpickle as pickle
            except:
                import pickle
            self.serializer = pickle.dumps
            self.deserializer = pickle.loads

    def binary( self, data ):
       return sqlite3.Binary( self.serializer(data) ) 

    def decode( self, data ):
        return self.deserializer(data)
          
    def setCallback(self, cb):
        self.callback = cb
          
    def getTable(self, clss):
        cName = clss.__name__
        if cName in self.tables:
            return cName
        self.tables.append(cName)
        try:
            if cName == 'Corpus' :
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, blob BLOB, ngrams BLOB)''')
            elif cName == 'AssocNGram' :
                self.execute('''create table '''+cName+''' (id1 VARCHAR, id2 VARCHAR, occs INTEGER, PRIMARY KEY (id1, id2))''')
            elif cName.startswith('Assoc'):
                self.execute('''create table '''+cName+''' (id1 VARCHAR, id2 VARCHAR, PRIMARY KEY (id1, id2))''')
            else:
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, blob BLOB)''')

            self.commit()
        except sqlite3.OperationalError, exc:
            # table already exists
            pass
        return cName

    def insert(self, id, obj ):
        req = 'insert into ' + self.getTable(obj.__class__) + ' values (?, ?)'
        try:
            self.execute(req,(id,self.binary(obj)))
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "OBJECT Operational error:",e2,req
            return False
        return True
        
    def insertAssoc(self, assoc):
        i1,i2 = assoc
        #i1,i2 = self.binary(i1), self.encode(i2)
        req = 'insert into ' + self.getTable(assoc.__class__) + ' values (?, ?)'
        try:
            self.execute(req, (i1,i2))
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "ASSOC Operational error:",e2,req
            return False
        return True
        
    def update(self, id, obj):
        req = 'update '+ self.getTable(obj.__class__) +' SET (blob = ?) WHERE (id LIKE ?)'
        self.execute(req, (self.binary(obj), id))
        self.commit()
        return True
      
    def fetch_all(self, clss):
        req = ('select * from '+self.getTable(clss))
        reply = self.execute(req)
        results = []
        for i, blob in reply:
            results.append( (id, self.decode(blob)) )
        return tuple (results )
        
    def fetch_one(self, clss, id):

        req = 'select * from '+ self.getTable(clss) + ' WHERE (id LIKE ?)'
        results = self.execute(req, [id])   

        i, blob = None, None
       
        for res in results:
            i, blob = res
            blob = self.decode(blob)
            break

        return blob
        
    def clean(self):
        """remove unused tables (empty)"""
        raise NotImplemented
        
    def clear(self):
        """clear all the tables"""
        for table in ['Document',
                      'Corpus',
                      'Corpora',
                      'AssocCorpus',
                      'AssocDocument',
                      ]:
            try:
                self.execute('DROP TABLE '+table) 
            except:
                pass  

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

    def execute(self, *args):
        return self.cursor().execute(*args)
           
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
        
    def storeDocument(self, id, document ):
        return self.insert( id, document )

    def storeCorpus(self, id, corpus, ngrams ):
        req = 'insert into ' + self.getTable(corpus.__class__) + ' values (?, ?, ?)'
        try:
            self.execute( req,( id, self.binary(corpus), self.binary(ngrams) ) )
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "OBJECT Operational error:",e2,req
            return False
        return True

#    def storeNGram(self, id, ngram ):
#        return self.insert( id, ngram )
#
    def storeAssocCorpus(self, corpusID, corporaID ):
        assoc = AssocCorpus((corpusID, corporaID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocDocument(self, docID, corpusID ):
        assoc = AssocDocument((docID, corpusID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocNGram(self, ngramID, docID, occs ):
        assoc = AssocNGram((ngramID, docID, occs))
        i1,i2,i3 = assoc
        #i1,i2,i3 = self.binary(i1), self.encode(i2), self.encode(i3)
        req = 'insert into ' + self.getTable(assoc.__class__) + ' values (?, ?, ?)'
        try:
            self.execute(req, (i1,i2,i3))
            self.commit()
        except sqlite3.IntegrityError, e1:
            print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            print "ASSOC Operational error:",e2,req
            return False
        return True

    def loadCorpora(self, id ):
        return self.fetch_one( Corpora, id )
        
    def loadDocument(self, id ):
        return self.fetch_one( Corpus, id )
        
    def loadCorpus(self, id ):
        req = 'select * from '+ self.getTable( Corpus ) + ' WHERE (id = ?)'
        results = self.execute(req, [id]).fetchone()
        if results is None:
            return None

        i, blob, ngrams = None, None, None
        i, blob, ngrams = results
        blob = self.decode(blob)
        ngrams = self.decode(ngrams)
        return {
            'id': i,
            'blob': blob,
            'ngrams': ngrams,
        }
        
    def loadCorpusNgrams(self, id ):
        req = 'select ngrams from '+ self.getTable( Corpus ) + ' WHERE (id = ?)'
        results = self.execute(req, [id]).fetchone()
        if results is None:
            return None
        return self.decode(str(results[0]))
        
    def loadAllAssocCorpus(self):
        return self.fetch_all( AssocCorpus )
             
    def loadAllAssocDocument(self):
        return self.fetch_all( AssocDocument )
             
    def loadAllAssocNGram(self):
        return self.fetch_all( AssocNGram )
   
    
    def getCorpora(self):
        corpora = self.fetch_all( Corpora )[0]
        print "corpora:",corpora
        
        
        
        
