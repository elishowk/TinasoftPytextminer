# -*- coding: utf-8 -*-

import PyTextMiner
from tinasoft.data.Relational import Api
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

        # sql api
        self.api = Api()
        self.path = path
        PyTextMiner.load_options(opts)
        self.lang,self.encoding = self.locale.split('.')
        self._db = sqlite3.connect(path)
        self.cursor = self._db.cursor
        self.commit = self._db.commit

        self.createTables()

        #if self.format == 'yaml':
        #    import yaml
        #    self.encoder = yaml.dump
        #    self.decoder = yaml.load
        #if self.format == 'json' or self.format == 'jsonpickle':
        #    import jsonpickle
        #    self.encoder = jsonpickle.encode
        #    self.decoder = jsonpickle.decode
        #else:
        #    try:
        #        import cpickle as pickle
        #    except:
        #        import pickle
        #    self.encoder = pickle.dumps
        #    self.decoder = pickle.loads

    def execute(self, *args):
        return self.cursor().execute(*args)

    def executemany(self, *args):
        return self.cursor().executemany(*args)
    
    def encode( self, data ):
       return sqlite3.Binary( PyTextMiner.encoder(data) ) 

    def decode( self, data ):
        # why str() : sqlite3 blob return unicode ?
        return PyTextMiner.decoder( str(data) )
    
    def createTables( self ):
        try:
            return self.execute( "".join(self.api.createTables()) )
        except sqlite3.IntegrityError, e1:
            print "OBJECT Integrity error:",e1,req
            return None
        except sqlite3.OperationalError, e2:
            print "OBJECT Operational error:",e2,req
            return None

    # TODO asynchrone calls
    #def setCallback(self, cb):
    #    self.callback = cb
          
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

    def insertCorpora(self, id, obj):
        req = self.api.inserCorpora()
        tuple = (id,self.encode(obj))
        self.safewrite( req, tuple )
    
    def insertCorpus(self, id, period_start, period_end, corpus):
        # id, period_start, period_end, blob
        req = self.api.insertCorpus()
        tuple = ( id, period_start, period_end, self.encode( corpus ) )
        self.safewrite( req, tuple )
    
    def insertmanyCorpus(self, iter):
        # id, period_start, period_end, blob
        req = self.api.insertCorpus()
        self.safewritemany( req, iter )

    def insertDocument(self, id, datestamp, obj):
        # id, timestamp, blob
        req = self.api.insertDocument()
        tuple = ( id, datestamp, self.encode(obj) )
        self.safewrite( req, tuple )
    
    def insertmanyDocument(self, iter):
        # id, datestamp, blob
        req = self.api.insertDocument()
        self.safewritemany( req, iter )

    def insertNGram(self, id, obj):
        req = self.api.inserNGram()
        tuple = (id,self.encode(obj))
        self.safewrite( req, tuple )
    
    def insertmanyNGram(self, obj, iter):
        req = self.api.insertNGram()
        self.safewritemany( req, iter )

    def insertAssoc(self, assocname, tuple ):
        req = self.api.insertAssoc( assocname )
        self.safewrite( req, tuple )
        
    def insertmanyAssoc(self, iter, assocname):
        req = self.api.insertAssoc( assocname )
        self.safewritemany( req, iter )
   
    def deletemanyAssoc( self, iter, assocname ): 
        req = self.api.deleteAssoc( assocname )
        self.safewritemany( req, iter )

    #def insertCooc(self, values):
    #    # id, period_start, period_end, ng id 1, ng id 2, cooccurences
    #    req = 'insert into '+self.getTable( Cooc )+' values (?, ?, ?, ?, ?, ?)'
    #    print "%s" % req
    #    self.safewrite( req, values )

    #def insertmanyCooc(self, iter):
    #    # id, period_start, period_end, ng id 1, ng id 2, cooccurences
    #    req = 'insert into '+self.getTable( Cooc )+' values (?, ?, ?, ?, ?, ?)'
    #    print "%s" % req
    #    self.safewritemany( req, iter )
    

    #def update(self, id, obj):
    #    req = 'update '+ self.getTable(obj.__class__) +' SET (blob = ?) WHERE (id LIKE ?)'
    #    tuple = (self.encode(obj), id)
    #    self.safewrite( req, tuple )
      
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
                self.execute('DROP TABLE IF EXISTS '+table) 
            except:
                pass
        self.createTables()

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

class Engine(SQLiteBackend):

    """
    For large tables (Assoc and NGram),
    please use the 'storemany' version
    """

    def storeCorpora(self,  id, corpora ):
        return self.insertCorpora( id, corpora )

    def storeCorpus(self, id, period_start, period_end, corpus ):
        return self.insertCorpus( id, period_start, period_end, corpus )

    def storeDocument(self, id, datestamp, document ):
        return self.insertDocument( id, datestamp, document )

    def storeNGram(self, id, ngram ):
        return self.insertNGram( id, ngram )
    
    def storemanyNGram( self, iter ):
        return self.insertmany( NGram(), iter )

    def storeAssocCorpus(self, corpusID, corporaID ):
        assoc = (corpusID, corporaID)
        return self.insertAssoc( 'AssocCorpus', assoc )
        
    def storeAssocDocument(self, docID, corpusID ):
        assoc = (docID, corpusID) 
        return self.insertAssoc( 'AssocDocument', assoc )
        
    def storeAssocNGramDocument(self, ngramID, docID, occs ):
        assoc = (ngramID, docID, occs)
        return self.insertAssoc( 'AssocNGramDocument', assoc )

    def storeAssocNGramCorpus(self, ngramID, corpID, occs ):
        assoc = (ngramID, corpID, occs)
        return self.insertAssoc( 'AssocNGramCorpus', assoc )

    def storemanyAssocNGramDocument( self, iter ):
        return self.insertmanyAssoc( iter, 'AssocNGramDocument' )
    
    def storemanyAssocNGramCorpus( self, iter ):
        return self.insertmanyAssoc( iter, 'AssocNGramCorpus' )
    
    def deletemanyAssocNGramDocument( self, iter ):
        return self.deletemanyAssoc( iter, 'AssocNGramDocument' )
    
    def deletemanyAssocNGramCorpus( self, iter ):
        return self.deletemanyAssoc( iter, 'AssocNGramCorpus' )

    def cleanAssocNGramDocument( self, corpusNum ):
        req = self.api.cleanAssocNGramDocument()
        arg = [corpusNum]
        return self.safewrite(req, arg)
    
    def loadCorpora(self, id ):
        req = self.api.loadCorpora()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res
        
    def loadCorpus(self, id ):
        req = self.api.loadCorpus()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], res[2], self.decode( res[3] ) ]
        else:
            return res
        
    def loadDocument(self, id ):
        req = self.api.loadDocument()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], res[1], self.decode( res[2] ) ]
        else:
            return res
        
    def loadNGram(self, id ):
        req = self.api.loadNGram()
        res = self.safereadone( req, [id] )
        if res is not None:
            return [ res[0], self.decode( res[1] ) ]
        else:
            return res

    def fetchCorpusNGram( self, corpusid ):
        req = self.api.fetchCorpusNGram()
        reply = self.safereadall(req, [corpusid])
        results = []
        for id, blob in reply:
            results.append( ( id, self.decode(blob) ) )
        return results
 
    def fetchCorpusNGramID( self, corpusid ):
        req = self.api.fetchCorpusNGramID()
        reply = self.safereadall(req, [corpusid])
        results = []
        for id in reply:
            results.append( self.decode(id[0]) )
        return results      

    def fetchDocumentNGram( self, documentid ):
        req = self.api.fetchDocumentNGram()
        reply = self.safereadall(req, [documentid])
        results = []
        for blob in reply:
            results.append( self.decode(blob[0]))
        return results

    def fetchDocumentNGramID( self, documentid ):
        req = self.api.fetchDocumentNGramID()
        reply = self.safereadall(req, [documentid])
        results = []
        for id in reply:
            results.append( id[0] )
        return results

    def fetchCorpusDocumentID( self, corpusid ): 
        req = self.api.fetchCorpusDocumentID()
        reply = self.safereadall(req, [corpusid])
        results = []
        for id in reply:
            results.append( id[0] )
        return results


