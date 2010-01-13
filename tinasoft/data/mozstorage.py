# -*- coding: utf-8 -*-
from tinasoft.data import Handler
import sqlapi

try:
    from xpcom import components, verbose, COMException, ServerException, nsError
except Exception, e:
    print "Unable to import xpcom"
    pass

class Engine(Handler):

    def __init__(self, path, **opts):
        self.api = sqlapi.Api()
        self._dbFileName = path
        self.loadOptions(opts)
        self._dbFile = None # holds the file name
        self._dbConn = None
        self._initialized = False
        self._storageService = components.classes["@mozilla.org/storage/service;1"].\
            getService(components.interfaces.mozIStorageService)
    
    def connect(self):
        try:
            dirService = components.classes["@mozilla.org/file/directory_service;1"]\
                .getService( components.interfaces.nsIProperties )
            self._dbFile = dirService.get( "ProfD", components.interfaces.nsIFile )
            self._dbFile.append( self._dbFilename )
            self._dbConn = self._storageService.openDatabase( self._dbFile )
            #print("createDB Error exception ", self._dbConn.lastErrorString )
            if self._dbConn.connectionReady:
                # create all tables
                sql = self.createTables()
                self._dbConn.executeSimpleSQL(" ".join(sql))
                # Setup commonly used SQL statements
                self._setupSQLStatements()
                # TODO: Increment the version number on DB schema change
                # this._dbConn.executeSimpleSQL("REPLACE INTO prefs VALUES ('DB_SCHEMA_VERSION', '0.0.1')");
                self._initialized = True
        except ServerException, e:
            print("catched : ", e)

    def _setupSQLStatements(self):
        # Select ALL tables
        sqlSelectAllTables = "SELECT name FROM sqlite_master WHERE type='table'"
        self.stmSelectAllTables = self._dbConn.createStatement(sqlSelectAllTables)
        self.insertCorpora = self._dbConn.createStatement(self.insertCorpora())
        self.insertCorpus = self._dbConn.createStatement(self.insertCorpus())
        self.insertDocument = self._dbConn.createStatement(self.insertDocument())
        self.insertNGram = self._dbConn.createStatement(self.insertNGram())
        self.insertAssocCorpus = self._dbConn.\
                createStatement(self.insertAssoc('AssocCorpus'))
        self.insertAssocDocument = self._dbConn.\
                createStatement(self.insertAssoc('AssocDocument'))
        self.insertAssocNGramCorpus = self._dbConn.\
                createStatement(self.insertAssoc('AssocNGramCorpus'))
        self.insertAssocNGramDocument = self._dbConn\
                .createStatement(self.insertAssoc('AssocNGramDocument'))
        self.deleteAssocCorpus = self._dbConn\
                .createStatement(self.deleteAssoc('AssocCorpus'))
        self.deleteAssocDocument = self._dbConn\
                .createStatement(self.deleteAssoc('AssocDocument'))
        self.deleteAssocNGramCorpus = self._dbConn\
                .createStatement(self.deleteAssoc('AssocNGramCorpus'))
        self.deleteAssocNGramDocument = self._dbConn\
                .createStatement(self.deleteAssoc('AssocNGramDocument'))
        self.loadCorpora = self._dbConn\
                .createStatement(self.loadCorpora())
        self.loadCorpus = self._dbConn\
                .createStatement(self.loadCorpus())
        self.loadDocument = self._dbConn\
                .createStatement(self.loadDocument())
        self.loadNGram = self._dbConn\
                .createStatement(self.loadNGram())
        self.cleanAssocNGramDocument = self._dbConn\
                .createStatement(self.cleanAssocNGramDocument())
        self.fetchCorpusNGram = self._dbConn\
                .createStatement(self.fetchCorpusNGram())
        self.fetchCorpusNGramID = self._dbConn\
                .createStatement(self.fetchCorpusNGramID())
        self.fetchDocumentNGram = self._dbConn\
                .createStatement(self.fetchDocumentNGram())
        self.fetchDocumentNGramID = self._dbConn\
                .createStatement(self.fetchDocumentNGramID())
        self.fetchCorpusDocumentID = self._dbConn\
                .createStatement(self.fetchCorpusDocumentID())
        print dir(self)

    def testXPCOM(self):
        cls = components.classes["Python.TestComponent"]
        ob = cls.createInstance(components.interfaces.nsIPythonTestInterfaceDOMStrings)
        print ob.GetStrings()
	
    # Drops and create brand new tables
    def resetDatabase(self):
        try:
            drop = "DROP TABLE IF EXISTS "
            tab = []
            while self.stmSelectAllTables.executeStep():
                tab.append( self.stmSelectAllTables.getString(0) )
                self.stmSelectAllTables.reset()
            for i in (length(tab)-1):
                self._dbConn.executeSimpleSQL( drop + tab[i] )
            self._dbConn.executeSimpleSQL(" ".join(self.createTables()))
            return True
        except Exception, e:
            print("resetDatabase Exception ", e)
            print( self._dbConn.lastErrorString )
            return False
