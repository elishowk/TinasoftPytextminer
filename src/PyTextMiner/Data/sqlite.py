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

    def execute(self, *args):
        return self.cursor().execute(*args)
    
    def executemany(self, *args):
        return self.cursor().executemany(*args)
    
    def encode( self, data ):
       return sqlite3.Binary( self.encoder(data) ) 

    def decode( self, data ):
        return self.decoder(str(data))
          
    def setCallback(self, cb):
        self.callback = cb
          
    def getTable(self, clss):
        cName = clss.__name__
        # if already exist, return
        if cName in self.tables:
            return cName
        # else create it
        self.tables.append(cName)
        try:
            if cName == 'Corpus':
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, period_start VARCHAR, period_end VARCHAR, blob BLOB)''')
            if cName == 'Document':
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, date VARCHAR, blob BLOB)''')
            if cName == 'Cooc':
                self.execute('''create table '''+cName+''' (id VARCHAR PRIMARY KEY, period_start VARCHAR, period_end VARCHAR, ngid1 VARCHAR, ngid2 VARCHAR, cooc INTEGER)''')
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

    def saferead( self, req, tuple ): 
        try:
            return self.execute(req, tuple)
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return None
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return None

    def safereadone( self, req, tuple ):
        read = self.saferead( req, tuple )
        return read.fetchone()

    def safereadall( self, req, tuple ):
        read = self.saferead( req, tuple )
        return read.fetchall()

    def safewrite( self, req, tuple ): 
        try:
            self.execute(req, tuple)
            self.commit()
        except sqlite3.IntegrityError, e1:
            #print "OBJECT Integrity error:",e1,req
            return False
        except sqlite3.OperationalError, e2:
            #print "OBJECT Operational error:",e2,req
            return False
        return True

    def safewritemany( self, req, iter ):
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

    def insert(self, id, obj):
        req = 'insert into ' + self.getTable(obj.__class__) + ' values (?, ?)'
        tuple = (id,self.encode(obj))
        self.safewrite( req, tuple )
    
    def insertmany(self, obj, iter):
        req = 'insert into ' + self.getTable( obj.__class__ ) + ' values (?, ?)'
        self.safewritemany( req, iter )

    def insertAssoc(self, assoc):
        req = 'insert into ' + self.getTable(assoc.__class__) + ' values (?, ?)'
        self.safewrite( req, assoc )
        
    def insertmanyAssoc(self, iter, assoc):
        myclass = self.getTable(assoc)
        if myclass.startswith('AssocNGram'):
            # ngid1, ngid2, occurences
            req = 'insert into ' + myclass + ' values (?, ?, ?)'
        else:
            req = 'insert into ' + myclass + ' values (?, ?)'
        self.safewritemany( req, iter )

    def insertCorpus(self, id, period_start, period_end, corpus):
        # id, period_start, period_end, blob
        req = 'insert into '+self.getTable(corpus.__class__)+' values (?, ?, ?, ?)'
        tuple = ( id, period_start, period_end, self.encode( corpus ) )
        self.safewrite( req, tuple )
    
    def insertmanyCorpus(self, iter):
        # id, period_start, period_end, blob
        req = 'insert into '+self.getTable(Corpus)+' values (?, ?, ?, ?)'
        self.safewritemany( req, iter )

    def insertDocument(self, id, timestamp, obj):
        # id, timestamp, blob
        req = 'insert into '+self.getTable(obj.__class__)+' values (?, ?, ?)'
        tuple = ( id, timestamp, self.encode(obj) )
        self.safewrite( req, tuple )
    
    def insertmanyDocument(self, iter):
        # id, timestamp, blob
        req = 'insert into '+self.getTable(Document)+' values (?, ?, ?)'
        self.safewritemany( req, iter )
       
    def insertCooc(self, values):
        # id, period_start, period_end, ng id 1, ng id 2, cooccurences
        req = 'insert into '+self.getTable( Cooc )+' values (?, ?, ?, ?, ?, ?)'
        print "%s" % req
        self.safewrite( req, values )

    def insertmanyCooc(self, iter):
        # id, period_start, period_end, ng id 1, ng id 2, cooccurences
        req = 'insert into '+self.getTable( Cooc )+' values (?, ?, ?, ?, ?, ?)'
        print "%s" % req
        self.safewritemany( req, iter )
    
    def deletemanyAssoc( self, iter, assoc ): 
        myclass = self.getTable(assoc.__class__)
        if myclass.startswith('AssocNGram'):
            req = 'delete from ' + myclass + ' where id1 = ?'
        else:
            req = 'delete from ' + myclass + ' where id1 = ?'
        self.safewritemany( req, iter )

    def update(self, id, obj):
        req = 'update '+ self.getTable(obj.__class__) +' SET (blob = ?) WHERE (id LIKE ?)'
        tuple = (self.encode(obj), id)
        self.safewrite( req, tuple )
      
    #def fetch_all(self, clss):
    #    req = ('select * from '+self.getTable(clss))
    #    reply = self.execute(req)
    #    results = []
    #    for i, blob in reply:
    #        results.append( (id, self.decode(blob)) )
    #    return tuple (results )
    #    
    #def fetch_one(self, clss, id):
    #    req = 'select * from '+ self.getTable(clss) + ' WHERE (id LIKE ?)'#%self.encode(id)
    #    results = self.execute(req, [id])   
    #    i, blob = None, None
    #   
    #    for res in results:
    #        i, blob = res
    #        #i = self.decode(i)
    #        blob = self.decode(blob)
    #        break

    #    return blob
        
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
                      'AssocNGramCorpus',
                      'Cooc']:
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

           
# APPLICATION LAYER
class Cooc (tuple):
    pass
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

    def storeCorpus(self, id, period_start, period_end, corpus ):
        return self.insertCorpus( id, period_start, period_end, corpus )

    def storeDocument(self, id, timestamp, document ):
        return self.insertDocument( id, timestamp, document )

    # deprecated        
    def storeNGram(self, id, ngram ):
        return self.insert( id, ngram )
    
    def storemanyNGram( self, iter ):
        return self.insertmany( NGram(), iter )

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
        req = 'delete from '+ self.getTable( AssocNGramDocument ) +' where id1 not in (select id1 from AssocNGramCorpus where id2 = ?)'
        arg = [corpusNum]
        return self.safewrite(req, arg)
    
    def loadCorpora(self, id ):
        req = 'SELECT id, blob FROM '+ self.getTable( Corpora ) +' WHERE id = ?'
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res
        
    def loadCorpus(self, id ):
        req = 'SELECT id, period_start, period_end, blob FROM '+ self.getTable( Corpus ) +' WHERE id = ?'
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], res[2], self.decode( res[3] ) ]
        else:
            return res
        
    def loadDocument(self, id ):
        req = 'SELECT id, date, blob FROM '+ self.getTable( Document ) +' WHERE id = ?'
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], self.decode( res[2] ) ]
        else:
            return res
        
    def loadNGram(self, id ):
        req = 'SELECT id, blob FROM  '+ self.getTable( NGram ) +' WHERE id = ?'
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res

    def fetchCorpusNGram( self, corpusid ):
        req = ('select id, blob from  '+ self.getTable( NGram ) +' as ng JOIN '+ self.getTable( AssocNGramCorpus ) +' as assoc ON assoc.id1=ng.id AND assoc.id2 = ?')
        reply = self.safereadall(req, [corpusid])
        results = []
        for id, blob in reply:
            results.append( ( id, self.decode(blob) ) )
        return results
 
    def fetchCorpusNGramID( self, corpusid ):
        req = ('select id1 from '+ self.getTable( AssocNGramCorpus ) +' where id2 = ?')
        reply = self.safereadall(req, [corpusid])
        results = []
        for id in reply:
            results.append( self.decode(id[0]) )
        return results      

    def fetchDocumentNGram( self, documentid ):
        req = ('select ng.blob from '+ self.getTable( NGram ) +' as ng JOIN '+ self.getTable( AssocNGramDocument ) +' as assoc ON assoc.id1=ng.id AND assoc.id2 = ?')
        reply = self.safereadall(req, [documentid])
        results = []
        for blob in reply:
            results.append( self.decode(blob[0]))
        return results

    def fetchDocumentNGramID( self, documentid ):
        req = ('select id1 from '+ self.getTable( AssocNGramDocument ) +' where id2 = ?')
        reply = self.safereadall(req, [documentid])
        results = []
        for id in reply:
            results.append( id[0] )
        return results

    def fetchCorpusDocumentID( self, corpusid ): 
        req = ('select id1 from '+ self.getTable( AssocDocument ) +' where id2 = ?')
        reply = self.safereadall(req, [corpusid])
        results = []
        for id in reply:
            results.append( id[0] )
        return results

    # deprecated
    def loadAllAssocCorpus(self):
        return self.fetch_all( AssocCorpus )
             
    # deprecated
    def loadAllAssocDocument(self):
        return self.fetch_all( AssocDocument )

    # deprecated
    def loadAllAssocNGramDocument(self):
        return self.fetch_all( AssocNGramDocument )
   
    # deprecated
    def loadAllAssocNGramCorpus(self):
        return self.fetch_all( AssocNGramCorpus )
    
    # deprecated
    def getCorpora(self):
        corpora = self.fetch_all( Corpora )[0]
        print "corpora:",corpora

