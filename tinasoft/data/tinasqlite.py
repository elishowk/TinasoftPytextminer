#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__="Elias Showk"
from tinasoft.data import Handler
from tinasoft.pytextminer import PyTextMiner

import sqlite3

from os.path import exists
from os.path import join
from os import makedirs

import cPickle as pickle

import logging
_logger = logging.getLogger('TinaAppLogger')

sqlite3.enable_callback_tracebacks(True)

# LOW-LEVEL BACKEND
class Backend(Handler):

    options = {
        'home' : 'db',
        'drop_tables': False,
        'prefix' : {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'NGram::',
            'Cooc':'Cooc::',
            'Whitelist':'Whitelist::',
            'Cluster':'Cluster::',
        },

        'tables' : [
            'Corpora',
            'Corpus',
            'Document',
            'NGram',
            'Cooc',
            'Whitelist',
            'Cluster'
        ],
    }

    def is_open(self):
        return self.__open

    def _drop_tables(self):
        try:
            cur = self._db.cursor()
            for tabname in self.tables:
                sql = "DROP TABLE IF EXISTS %s"%tabname
                cur.execute(sql)
            self._db.commit()
        except Exception, exc:
            _logger.error("_drop_tables() error : %s"%exc)
            raise Exception(exc)

    def _connect(self):
        """connection method, need to have self.home directory created"""
        try:
            self._db = sqlite3.connect(join(self.home,self.path))
            #self._db.execute("PRAGMA SYNCHRONOUS=0")
            # row factory provides named columns
            self._db.row_factory = sqlite3.Row
            self.__open = True
        except Exception, connect_exc:
            _logger.error("_connect() error : %s"%connect_exc)
            raise Exception(connect_exc)

    def _create_tables(self):
        try:
            cur = self._db.cursor()
            sql = "PRAGMA SYNCHRONOUS=0;"
            cur.execute(sql)
            for tabname in self.tables:
                sql = "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(256) PRIMARY KEY, pickle BLOB)"%tabname
                cur.execute(sql)
            self._db.commit()
        except Exception, connect_exc:
            _logger.error("_connect() error : %s"%connect_exc)
            raise Exception(connect_exc)

    def __init__(self, path, **opts):
        """loads options, connect or create db"""
        self.loadOptions(opts)
        self.path = path
        if not exists(self.home):
            makedirs(self.home)
        self._connect()
        if self.drop_tables is True:
            _logger.debug("will drop all tables")
            self._drop_tables()
        self._create_tables()
        if self.is_open() is False:
            raise Exception("Unable to open a tinasqlite database")

    def close(self, commit_pending_transaction=True):
        """
        Properly handles transactions explicitely (with parameter) or by default

        """
        if not self.is_open():
            return
        self.__closing = True
        self._db.close()
        self.__open = False

    def commit(self):
        """
        commits
        """
        try:
            self._db.commit()
        except Exception, e:
            self.rollback()

    def rollback(self):
        """
        rollbacks
        """
        self._db.rollback()
        _logger.warning("rolled back an sql statement")

    def pickle( self, obj ):
        return pickle.dumps(obj)

    def unpickle( self, data ):
        if type(data) != str:
            data = str(data)
        return pickle.loads(data)

    def saferead(self, tabname, key):
        """returns ONE db value or return None"""
        if type(key) != str:
            key = str(key)
        try:
            cur = self._db.cursor()
            cur.execute("select pickle from %s where id=?"%tabname, (key,))
            row = cur.fetchone()
            if row is not None:
                return row["pickle"]
            else:
                return None
        except Exception, read_exc:
            _logger.error( "saferead exception : %s"%read_exc )
            return None

    def safereadrange(self, tabname):
        """returns a cursor of a whole table"""
        try:
            cur = self._db.cursor()
            cur.execute("select id, pickle from %s"%tabname)
            next_val = cur.fetchone()
            while next_val is not None:
                yield next_val
                next_val = cur.fetchone()
            return
        except Exception, readrange_exc:
            _logger.error( "exception during safereadrange on table %s : %s"%(tabname,readrange_exc) )
            return

    def safewrite( self, tabname, list_of_tuples ):
        """
        Pickles a list of tuples
        then execute many inserts of this transformed list of tuples
        """
        pickled_list = [(key,buffer(self.pickle(obj))) for (key, obj) in list_of_tuples]
        try:
            cur = self._db.cursor()
            cur.executemany("insert or replace into %s(id, pickle) values(?,?)"%tabname, pickled_list)
            self.commit()
        except Exception, insert_exc:
            _logger.error( "exception during safewrite on table %s : %s"%(tabname,insert_exc) )
            self.rollback()

    def safedelete(self, tabname, key):
        """
        TODO
        """
        pass

class Engine(Backend):
    """
    tinabsddb Engine
    """
    ngramqueue = []
    ngramqueueindex = []
    coocqueue = []
    MAX_INSERT_QUEUE = 500
    ngramindex = []


    def __del__(self):
        """safely closes db and dbenv"""
        self.flushQueues()
        self.close()

    def delete(self, id, deltype):
        """deletes a object given its type and id"""
        self.safedelete(deltype, id)

    def load(self, id, target, raw=False):
        read = self.saferead( target, id )
        if read is not None:
            if raw is True:
                return read
            return self.unpickle(str(read))
        else:
            return None

    def loadMany(self, target, raw=False):
        """
        returns a generator of tuples (id, pickled_obj)
        """
        cursor = self.safereadrange(target)
        try:
            while 1:
                record = cursor.next()
                # if cursor is empty
                if record is None: return
                if not raw:
                    yield ( record["id"], self.unpickle(str(record["pickle"])))
                else:
                    yield record
        except StopIteration, si:
            return

    def loadCorpora(self, id, raw=False ):
        return self.load(id, 'Corpora', raw)

    def loadCorpus(self, id, raw=False ):
        return self.load(id, 'Corpus', raw)

    def loadDocument(self, id, raw=False ):
        return self.load(id, 'Document', raw)

    def loadNGram(self, id, raw=False ):
        return self.load(id, 'NGram', raw)

    def loadCooc(self, id, raw=False ):
        return self.load(id, 'Cooc', raw)

    def loadWhitelist(self, id, raw=False):
        return self.load(id, 'Whitelist', raw)

    def loadCluster(self, id, raw=False):
        return self.load(id, 'Cluster', raw)

    def insertMany(self, iter, target, overwrite=False):
        """TODO overwrite is ignored"""
        if len(iter) != 0:
            return self.safewrite(target, iter)

    def insert( self, obj, target, id=None, overwrite=False ):
        if id is None:
            id = obj['id']
        return self.safewrite( target, [(id, obj)] )

    def insertCorpora(self, obj, id=None, overwrite=False ):
        return self.insert( obj, 'Corpora', id, overwrite )

    def insertCorpus(self, obj, id=None, overwrite=False ):
        return self.insert( obj, 'Corpus', id, overwrite )

    def insertManyCorpus(self, iter, overwrite=False ):
        return self.insertMany( iter, 'Corpus', overwrite )

    def insertDocument(self, obj, id=None, overwrite=False ):
        """automatically removes text content before storing"""
        return self.insert( obj, 'Document', id, overwrite )

    def insertManyDocument(self, iter, overwrite=False ):
        return self.insertMany( iter, 'Document', overwrite )

    def insertNGram(self, obj, id=None, overwrite=False ):
        return self.insert( obj, 'NGram', id, overwrite )

    def insertManyNGram(self, iter, overwrite=False ):
        return self.insertMany( iter, 'NGram', overwrite )

    def insertCooc(self, obj, id, overwrite=False ):
        # @id "corpus_id::ngramd_id"
        # @obj Cooc class instance
        return self.insert( obj, 'Cooc', id, overwrite )

    def insertManyCooc( self, iter, overwrite=False ):
        return self.insertMany( iter, 'Cooc', overwrite )

    def insertWhitelist(self, obj, id, overwrite=False ):
        return self.insert( obj, 'Whitelist', id, overwrite )

    def insertManyWhitelist( self, iter, overwrite=False ):
        return self.insertMany( iter, 'Whitelist', overwrite )

    def insertCluster(self, obj, id, overwrite=False ):
        return self.insert( obj, 'Cluster', id, overwrite )

    def insertManyCluster( self, iter, overwrite=False ):
        return self.insertMany( iter, 'Cluster', overwrite )

    def selectCorpusCooc(self, corpusId):
        """
        Yields tuples (ngramkey, cooc_matrix_line) from table Cooc
        for a given a corpus
        """
        #if isinstance(corpusId, str) is False:
        #    corpusId = str(corpusId)
        coocGen = self.select( 'Cooc', corpusId )
        try:
            record = coocGen.next()
            while record:
                # separate CORPUSID::NGRAMID
                key = record[0].split('::')
                # yields ngram id associated with its Cooc obj
                yield (key[1], record[1])
                record = coocGen.next()
        except StopIteration, si:
            return


    def select( self, tabname, key=None, raw=False ):
        """
        Yields raw or unpickled tuples (key, obj)
        from a table filtered with a range of key prefix
        """
        cursor = self.safereadrange( tabname )
        try:
            while 1:
                record = cursor.next()
                # if cursor is empty
                if record is None: return
                # if the record does not belong to the corpus_id
                if key is not None and record["id"].startswith(key) is False:
                    continue
                # otherwise yields the next value
                if raw is True:
                    yield record
                else:
                    yield ( record["id"], self.unpickle(str(record["pickle"])))
        except StopIteration, si:
            return

    def updateWhitelist( self, whitelistObj, overwrite ):
        """updates or overwrite a Whitelist and associations"""
        if overwrite is True:
            self.insertWhitelist( whitelistObj, overwrite=True )
            return
        stored = self.loadWhitelist( whitelistObj['id'] )
        if stored is not None:
            whitelistObj = PyTextMiner.updateObjectEdges( whitelistObj, stored )
        self.insertWhitelist( whitelistObj, overwrite=True )

    def updateCluster( self, obj, overwrite ):
        """updates or overwrite a Cluster and associations"""
        if overwrite is True:
            self.insertCluster( obj, overwrite=True )
            return
        stored = self.loadCluster( obj['id'] )
        if stored is not None:
            obj = PyTextMiner.updateObjectEdges( obj, stored )
        self.insertCluster( obj, overwrite=True )


    def updateCorpora( self, corporaObj, overwrite ):
        """updates or overwrite a corpora and associations"""
        if overwrite is True:
            self.insertCorpora( corporaObj, overwrite=True )
            return
        storedCorpora = self.loadCorpora( corporaObj['id'] )
        if storedCorpora is not None:
            corporaObj = PyTextMiner.updateObjectEdges( corporaObj, storedCorpora )
        self.insertCorpora( corporaObj, overwrite=True )

    def updateCorpus( self, corpusObj, overwrite ):
        """updates or overwrite a corpus and associations"""
        if overwrite is True:
            self.insertCorpus( corpusObj, overwrite=True )
            return
        storedCorpus = self.loadCorpus( corpusObj['id'] )
        if storedCorpus is not None:
            corpusObj = PyTextMiner.updateObjectEdges( corpusObj, storedCorpus )
        self.insertCorpus( corpusObj, overwrite=True )

    def updateDocument( self, documentObj, overwrite ):
        """updates or overwrite a document and associations"""
        if overwrite is True:
            self.insertDocument( documentObj, overwrite=True )
            return
        storedDocument = self.loadDocument( documentObj['id'] )
        if storedDocument is not None:
            documentObj = PyTextMiner.updateObjectEdges( documentObj, storedDocument )
        self.insertDocument( documentObj, overwrite=True )
        # returns a duplicate information
        if storedDocument is not None:
            return [storedDocument]
        else:
            return []

    def flushNGramQueue(self):
        self.insertManyNGram( self.ngramqueue, overwrite=True )
        self.ngramqueue = []
        self.ngramqueueindex = []

    def flushCoocQueue(self):
        self.insertManyCooc( self.coocqueue, overwrite=True )
        self.coocqueue = []

    def flushQueues(self):
        self.flushCoocQueue()
        self.flushNGramQueue()
        self.ngramindex= []
        _logger.debug("flushed insert queues for database %s"%self.path)

    def _ngramQueue( self, id, ng ):
        """
        add a ngram to the queue and session index
        """
        self.ngramqueueindex += [id]
        self.ngramqueue += [(id, ng)]
        self.ngramindex += [id]
        queue = len( self.ngramqueue )
        return queue


    def updateNGram( self, ngObj, overwrite, docId=None, corpId=None ):
        """updates or overwrite a ngram and associations"""

        # if ngram is already into queue, will flush it first to permit incremental updates
        if ngObj['id'] in self.ngramqueueindex:
            self.flushNGramQueue()
        # then try to update the NGram
        storedNGram = self.loadNGram( ngObj['id'] )
        if storedNGram is not None:
            # if overwriting and existing NGram yet NOT in the current index
            if overwrite is True and ngObj['id'] not in self.ngramindex:
                # cleans current corpus edges unless it won't change during overwrite
                if corpId is not None and corpId in storedNGram['edges']['Corpus']:
                    del storedNGram['edges']['Corpus'][corpId]
                # cleans current document edges unless it won't change during overwrite
                if  docId is not None and docId in storedNGram['edges']['Document']:
                    del storedNGram['edges']['Document'][docId]
            # anyway, updates all edges
            ngObj = PyTextMiner.updateObjectEdges( storedNGram, ngObj )
        # adds object to the INSERT the queue
        return self._ngramQueue( ngObj['id'], ngObj )

    def _coocQueue( self, id, obj, overwrite ):
        """
        Transaction queue grouping by self.MAX_INSERT_QUEUE
        overwrite should always be True because updateNGram
        keep the object updated
        """
        self.coocqueue += [(id, obj)]
        queue = len( self.coocqueue )
        if queue > self.MAX_INSERT_QUEUE:
            _logger.debug("will flushCoocQueue")
            self.flushCoocQueue()
            return 0
        else:
            return queue

    def updateCooc( self, id, obj, overwrite ):
        """updates or overwrite Cooc row"""
        return self._coocQueue( id, obj, overwrite )


