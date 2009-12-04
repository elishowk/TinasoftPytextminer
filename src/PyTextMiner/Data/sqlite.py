# -*- coding: utf-8 -*-

from PyTextMiner import Corpus, Document, Corpora, Data, NGram
import sqlite3

# LOW-LEVEL BACKEND 
class SQLiteBackend (Data.Handler):

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
            self.encoder = yaml.dump
            self.decoder = yaml.load
        elif self.format == 'json' or self.format == 'jsonpickle':
            import jsonpickle
            self.encoder = jsonpickle.encode
            self.decoder = jsonpickle.decode
        else:
            try:
                import cpickle as pickle
            except:
                import pickle
            self.encoder = pickle.dumps
            self.decoder = pickle.loads

    def encode( self, data ):
       return sqlite3.Binary( self.encoder(data) ) 

    def decode( self, data ):
        return self.decoder(str(data))
          
    def setCallback(self, cb):
        self.callback = cb
          
    def getTable(self, clss):
        cName = clss.__name__
        if cName in self.tables:
            return cName
        self.tables.append(cName)
        try:
            if cName.startswith('AssocNGram') :
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

    def insert(self, id, obj):
        req = 'insert into ' + self.getTable(obj.__class__) + ' values (?, ?)'
        try:
            self.execute(req,(id,self.encode(obj)))
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "OBJECT Operational error:",e2,req
            return False
        return True
        
    def insertmany(self, obj, iter):
        req = 'insert into ' + self.getTable( obj.__class__ ) + ' values (?, ?)'
        #def encodeObj( item ):
        #    return ( item[0], self.encode( item[1] ) )
        try:
            self.executemany( req, iter )
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
        #i1,i2 = self.encode(i1), self.encode(i2)
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
        
    def insertmanyAssoc(self, iter, assoc):
        myclass = self.getTable(assoc)
        if myclass.startswith('AssocNGram'):
            req = 'insert into ' + myclass + ' values (?, ?, ?)'
        else:
            req = 'insert into ' + myclass + ' values (?, ?)'
        try:
            self.executemany(req, iter)
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "ASSOC Operational error:",e2,req
            return False
        return True

    def deletemanyAssoc( self, iter, assoc ): 
        myclass = self.getTable(assoc.__class__)
        if myclass.startswith('AssocNGram'):
            req = 'delete from ' +  + ' where id1 = ?'
        else:
            req = 'delete from ' + myclass + ' where id1 = ?'
        try:
            self.executemany(req, iter)
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
        self.execute(req, (self.encode(obj), id))
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

        req = 'select * from '+ self.getTable(clss) + ' WHERE (id LIKE ?)'#%self.encode(id)
        results = self.execute(req, [id])   

        i, blob = None, None
       
        for res in results:
            i, blob = res
            #i = self.decode(i)
            blob = self.decode(blob)
            break

        return blob
        
    def clean(self):
        """remove unused tables (empty)"""
        raise NotImplemented
        
    def clear(self):
        """clear all the tables"""
        for table in ['NGram',
                      'Document',
                      'Corpus',
                      'Corpora',
                      'AssocCorpus',
                      'AssocDocument',
                      'AssocNGramDocument',
                      'AssocNGramCorpus']:
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
    
    def executemany(self, *args):
        return self.cursor().executemany(*args)
           
# APPLICATION LAYER
class Assoc (tuple):
    pass
class AssocCorpus (Assoc):
    pass
class AssocDocument (Assoc):
    pass
class AssocNGramDocument (Assoc):
    pass
class AssocNGramCorpus (Assoc):
    pass

class Engine(SQLiteBackend):

    def storeCorpora(self,  id, corpora ):
        return self.insert( id, corpora )
        
    def storeCorpus(self, id, corpus ):
        return self.insert( id, corpus )

    def storeDocument(self, id, document ):
        return self.insert( id, document )

    def storeNGram(self, id, ngram ):
        return self.insert( id, ngram )
    
    def storemanyNGram( self, iter ):
        return self.insertmany( NGram(), iter)

    def storeAssocCorpus(self, corpusID, corporaID ):
        assoc = AssocCorpus((corpusID, corporaID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocDocument(self, docID, corpusID ):
        assoc = AssocDocument((docID, corpusID)) 
        return self.insertAssoc( assoc )
        
    def storeAssocNGramDocument(self, ngramID, docID, occs ):
        assoc = AssocNGramDocument((ngramID, docID, occs))
        return self.insertAssoc( assoc )

    def storeAssocNGramCorpus(self, ngramID, corpID, occs ):
        assoc = AssocNGramCorpus((ngramID, corpID, occs))
        return self.insertAssoc( assoc )

    def storemanyAssocNGramDocument( self, iter ):
        return self.insertmanyAssoc( iter, AssocNGramDocument )
    
    def storemanyAssocNGramCorpus( self, iter ):
        return self.insertmanyAssoc( iter, AssocNGramCorpus )
    
    def deletemanyAssocNGramDocument( self, iter ):
        return self.deletemanyAssoc( iter, AssocNGramDocument )
    
    def deletemanyAssocNGramCorpus( self, iter ):
        return self.deletemanyAssoc( iter, AssocNGramCorpus )

    def cleanAssocNGramDocument( self, corpusNum ):
        req = 'delete from AssocNGramDocument where id1 not in (select id1 from AssocNGramCorpus where id2 = ?)'
        try:
            self.execute(req, [corpusNum])
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "ASSOC ntegrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "ASSOC Operational error:",e2,req
            return False
        return True
    
    def loadCorpora(self, id ):
        return self.fetch_one( Corpora, id )
        
    def loadCorpus(self, id ):
        return self.fetch_one( Corpus, id )
        
    def loadDocument(self, id ):
        return self.fetch_one( Document, id )
        
    def loadNGram(self, id ):
        return self.fetch_one( NGram, id )

    def fetchCorpusNGram( self, corpusid ):
        req = ('select id, blob from NGram as ng JOIN AssocNGramCorpus as assoc ON assoc.id1=ng.id AND assoc.id2 = ?')
        reply = self.execute(req, [corpusid]).fetchall()
        results = []
        for id, blob in reply:
            results.append( (id, self.decode(blob)) )
        return results
 
    def fetchCorpusNGramID( self, corpusid ):
        req = ('select id1 from AssocNGramCorpus where id2 = ?')
        reply = self.execute(req, [corpusid]).fetchall()
        results = []
        for id in reply:
            results.append( self.decode(id[0]) )
        return results      

    def fetchDocumentNGram( self, documentid ):
        req = ('select ng.blob from NGram as ng JOIN AssocNGramDocument as assoc ON assoc.id1=ng.id AND assoc.id2 = ?')
        reply = self.execute(req, [documentid]).fetchall()
        results = []
        for blob in reply:
            results.append( self.decode(blob[0]))
        return results

    def fetchDocumentNGramID( self, documentid ):
        req = ('select id1 from AssocNGramDocument where id2 = ?')
        reply = self.execute(req, [documentid]).fetchall()
        results = []
        for id in reply:
            results.append( id[0] )
        return results

    def fetchCorpusDocumentID( self, corpusid ): 
        req = ('select id1 from AssocDocument where id2 = ?')
        reply = self.execute(req, [corpusid]).fetchall()
        results = []
        for id in reply:
            results.append( id[0] )
        return results

    def loadAllAssocCorpus(self):
        return self.fetch_all( AssocCorpus )
             
    def loadAllAssocDocument(self):
        return self.fetch_all( AssocDocument )

    def loadAllAssocNGramDocument(self):
        return self.fetch_all( AssocNGramDocument )
   
    def loadAllAssocNGramCorpus(self):
        return self.fetch_all( AssocNGramCorpus )
    
    def getCorpora(self):
        corpora = self.fetch_all( Corpora )[0]
        print "corpora:",corpora

