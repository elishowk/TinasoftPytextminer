# -*- coding: utf-8 -*-

class Api():
    """
    SQL API, defines sql statements and abstract methods
    """

    tables = ['Corpora', 'Corpus', 'Document', 'NGram', 'AssocCorpus', 'AssocDocument', 'AssocNGramDocument', 'AssocNGramCorpus']

    def createTablesStmt(self):
        tables = []
        tables.append("create table if not exists Corpora (id VARCHAR PRIMARY KEY, blob BLOB);")
        tables.append("create table if not exists Corpus (id VARCHAR PRIMARY KEY, period_start VARCHAR, period_end VARCHAR, blob BLOB);")
        tables.append("create table if not exists Document (id VARCHAR PRIMARY KEY, date VARCHAR, blob BLOB);")
        tables.append("create table if not exists NGram (id VARCHAR PRIMARY KEY, blob BLOB);")
        tables.append("create table if not exists AssocNGramDocument (id1 VARCHAR, id2 VARCHAR, occs INTEGER, PRIMARY KEY (id1, id2));")
        tables.append("create table if not exists AssocNGramCorpus (id1 VARCHAR, id2 VARCHAR, occs INTEGER, PRIMARY KEY (id1, id2));")
        tables.append("create table if not exists AssocCorpus (id1 VARCHAR, id2 VARCHAR, PRIMARY KEY (id1, id2));")
        tables.append("create table if not exists AssocDocument (id1 VARCHAR, id2 VARCHAR, PRIMARY KEY (id1, id2));")
        return tables

    def createTables(self):
        raise NotImplementedError

    def dropTablesStmt(self):
        stmt=[]
        for table in self.tables:
            stmt.append('DROP TABLE IF EXISTS '+table)
        return stmt

    def dropTables(self):
        raise NotImplementedError

    def insertCorporaStmt(self):
        req = 'insert into Corpora values (?1, ?2)'
        return req

    def insertCorpora(self, id, obj):
        raise NotImplementedError

    def insertCorpusStmt(self):
        # id, period_start, period_end, blob
        req = 'insert into Corpus values (?1, ?2, ?3, ?4)'
        return req

    def insertCorpus(self, id, period_start, period_end, corpus):
        raise NotImplementedError

    def insertmanyCorpus(self, iter):
        raise NotImplementedError

    def insertDocumentStmt(self):
        req = 'insert into Document values (?1, ?2, ?3)'
        return req

    def insertDocument(self, id, datestamp, obj):
        raise NotImplementedError

    def insertmanyDocument(self, iter):
        raise NotImplementedError

    def insertNGramStmt(self):
        req = 'insert into NGram values (?1, ?2)'
        return req

    def insertNGram(self, id, obj):
        raise NotImplementedError

    def insertmanyNGram(self, iter):
        raise NotImplementedError

    def insertAssocStmt(self, myclassname):
        if myclassname.startswith('AssocNGram'):
            # ngid1, ngid2, occurences
            req = 'insert into ' + myclassname + ' values (?1, ?2, ?3)'
        else:
            req = 'insert into ' + myclassname + ' values (?1, ?2)'
        return req

    def insertAssoc(self, assocname, tuple ):
        raise NotImplementedError

    def insertmanyAssoc(self, iter, assocname):
        raise NotImplementedError

    def deleteAssocStmt( self, myclassname ):
        if myclassname.startswith('AssocNGram'):
            req = 'delete from ' + myclassname + ' where id1 = ?1'
        else:
            req = 'delete from ' + myclassname + ' where id1 = ?1'
        return req

    def deletemanyAssoc( self, iter, assocname ):
        raise NotImplementedError

    def deletemanyAssocNGramCorpus( self, iter ):
        raise NotImplementedError

    def cleanAssocNGramDocumentStmt(self):
        req = 'delete from AssocNGramDocument' \
                +' where id1 not in (select id1 from AssocNGramCorpus where id2 = ?1)'
        return req

    def cleanAssocNGramDocument( self, corpusNum ):
        raise NotImplementedError

    def loadCorporaStmt(self):
        req = 'SELECT id, blob FROM Corpora WHERE id = ?1'
        return req

    def loadCorpusStmt(self):
        req = 'SELECT id, period_start, period_end, blob FROM Corpus' \
                +' WHERE id = ?1'
        return req

    def loadDocumentStmt(self):
        req = 'SELECT id, date, blob FROM Document WHERE id = ?1'
        return req

    def loadNGramStmt(self):
        req = 'SELECT id, blob FROM NGram WHERE id = ?1'
        return req

    def loadCorpora(self, id ):
        raise NotImplementedError

    def loadCorpus(self, id ):
        raise NotImplementedError

    def loadDocument(self, id ):
        raise NotImplementedError

    def loadNGram(self, id ):
        raise NotImplementedError

    def fetchCorpusNGramStmt(self):
        req = ('select ng.blob from NGram as ng' \
                +' JOIN AssocNgramCorpus as assoc' \
                +' ON assoc.id1=ng.id WHERE assoc.id2 = ?1')
        return req

    def fetchCorpusNGram( self, corpusid ):
         raise NotImplementedError

    def fetchCorpusNGramIDStmt(self):
        req = ('select id1 from AssocNGramCorpus' \
                +' where id2 = ?1')
        return req

    def fetchCorpusNGramID( self, corpusid ):
        raise NotImplementedError

    def fetchDocumentNGramStmt(self):
        req = ('select ng.blob from NGram' \
                +' as ng JOIN AssocNGramDocument' \
                +' as assoc ON assoc.id1=ng.id AND assoc.id2 = ?1')
        return req

    def fetchDocumentNGram( self, documentid ):
        raise NotImplementedError

    def fetchDocumentNGramIDStmt(self):
        req = ('select id1 from AssocNGramDocument' \
                +' where id2 = ?1')
        return req

    def fetchDocumentNGramID( self, documentid ):
        raise NotImplementedError

    def fetchCorpusDocumentIDStmt(self):
        req = ('select id1 from AssocDocument' \
                +' where id2 = ?1')
        return req

    def fetchCorpusDocumentID( self, corpusid ):
        raise NotImplementedError

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
