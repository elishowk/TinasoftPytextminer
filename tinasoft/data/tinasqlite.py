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

import sqlite3
import os
from os.path import exists
from os.path import join
from os import makedirs

import cPickle as pickle
#from time import sleep, time

import logging
_logger = logging.getLogger('TinaAppLogger')


# LOW-LEVEL BACKEND
class Backend(Handler):

    options = {
        #'locale'        : 'en_US.UTF-8',
        #'format'        : 'jsonpickle',
        #'debug'         : False,
        #'compression'   : None,
        'home'          : 'db',
        'prefix'        : {
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

    def _connect(self):
        """connection method, need to have self.home directory created"""
        try:
            self._db = sqlite3.connect(join(self.home,self.path))
            self._db.execute("PRAGMA SYNCHRONOUS = OFF;")
            self._db.row_factory = sqlite3.Row
            cur = self._db.cursor()
            for tabname in self.tables:
                sql = "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(256) PRIMARY KEY, pickle BLOB)"%tabname
                cur.execute(sql)
            self._db.commit()
            self.__open = True
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
        _logger.error("rolling back")


    def pickle( self, obj ):
        return pickle.dumps(obj)

    def unpickle( self, data ):
        return pickle.loads(data)

    def saferead(self, tabname, key):
        """returns ONE db value or return None"""
        if isinstance(key, str) is False:
            key = str(key)
        try:
            cur = self._db.cursor()
            #_logger.debug("select * from %s where id=?"%tabname)
            #_logger.debug(key)
            cur.execute("select pickle from %s where id=?"%tabname, (key,))
            row = cur.fetchone()
            if row is not None:
                #_logger.debug(type(row["pickle"]))
                return row["pickle"]
            else:
                return None
        except Exception, read_exc:
            _logger.error( "saferead exception : %s"%read_exc )
            return None

    def safereadrange( self, tabname):
        """returns a cursor of a whole table"""
        if isinstance(key, str) is False:
            key = str(key)
        try:
            cur = self._db.cursor()
            cur.execute("select id, pickle from %s"%tabname)
            if cur.row_count == 0:
                return
            else:
                next_val = 1
                while next_val is not None:
                    next_val = cur.fetchone()
                    yield next_val
                return
        except Exception, readrange_exc:
            _logger.error( "saferead exception : %s"%readrange_exc )
            return

    def safewrite( self, tabname, list_of_tuples ):
        """
        Pickles a list of tuples
        then execute many inserts of this transformed list of tuples
        """
        pickled_list = [(str(key),self.pickle(obj)) for (key, obj) in list_of_tuples]
        try:
            cur = self._db.cursor()
            cur.executemany("insert or replace into %s(id, pickle) values(?,?)"%tabname, pickled_list)
            self.commit()
        except Exception, insert_exc:
            _logger.error( "safewrite exception : %s"%insert_exc )
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
        #try:
        self.flushQueues()
        self.close()
        #except Exception, e:
        #    _logger.error( "tinasqlite.Engine.__del__ exception " )
        #     raise Exception(e)

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
        return self.safereadrange(target)

    def loadCorpora(self, id, raw=False ):
        return self.load(id, 'Corpora', raw)

    def loadCorpus(self, id, raw=False ):
        return self.load(id, 'Corpus', raw)

    def loadManyDocument(self):
        """
        gets a generator of the whole table
        yields filtered rows based on a list of id
        """
        readmany = self.load('Document')
        try:
            record = readmany.next()
        except StopIteration, si:
            return

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
        #_logger.debug(target)
        #_logger.debug(iter)
        if len(iter) != 0:
            return self.safewrite(target, iter)

    def insert( self, obj, target, id=None, overwrite=False ):
        """TODO overwrite is ignored"""
        if id is None:
            id = obj['id']
        #_logger.debug(target)
        #_logger.debug([(id, obj)])
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
        """Yields a view on Cooc values given a period=corpus"""
        if isinstance(corpusId, str) is False:
            corpusId = str(corpusId)
        coocGen = self.select( 'Cooc', corpusId )
        try:
            record = coocGen.next()
            while record:
                key = record[0].split('::')
                # yields ngram id associated with its Cooc obj
                yield ( key[2], record[1])
                record = coocGen.next()
        except StopIteration, si:
            return


    def select( self, tabname, key=None, raw=False ):
        """Yields raw or unpickled tuples from a range of key"""
        cursor = self.safereadrange( tabname )
        try:
            record = cursor.next()
            while 1:
                # if cursor is empty
                if record is None: return
                # if the record does not belong to the corpus_id
                if minkey is not None and record["id"].startswith(key) is False:
                    continue
                # otherwise yields the next value
                if raw is True:
                    yield record
                else:
                    yield ( record["id"], self.unpickle(record["pickle"]))
                record = cursor.next()
        except StopIteration, si:
            return

    def updateEdges(self, canditate, update, types):
        """updates an existent object's edges with the candidate object's edges"""
        for targets in types:
            for targetsId, targetWeight in canditate['edges'][targets].iteritems():
                res = update.addEdge( targets, targetsId, targetWeight )
                #if res is False:
                    #_logger.debug( "%s addEdge refused, target type = %s, %s" \
                    #    %(update.__class__.__name__,targets, targetsId) )
        return update

    def updateWhitelist( self, whitelistObj, overwrite ):
        """updates or overwrite a Whitelist and associations"""
        if overwrite is True:
            self.insertWhitelist( whitelistObj, overwrite=True )
            return
        stored = self.loadWhitelist( whitelistObj['id'] )
        if stored is not None:
            whitelistObj = self.updateEdges( whitelistObj, stored, ['NGram'] )
        self.insertWhitelist( whitelistObj, overwrite=True )

    def updateCluster( self, obj, overwrite ):
        """updates or overwrite a Cluster and associations"""
        if overwrite is True:
            self.insertCluster( obj, overwrite=True )
            return
        stored = self.loadCluster( obj['id'] )
        if stored is not None:
            obj = self.updateEdges( obj, stored, ['NGram'] )
        self.insertCluster( obj, overwrite=True )


    def updateCorpora( self, corporaObj, overwrite ):
        """updates or overwrite a corpora and associations"""
        if overwrite is True:
            self.insertCorpora( corporaObj, overwrite=True )
            return
        storedCorpora = self.loadCorpora( corporaObj['id'] )
        if storedCorpora is not None:
            corporaObj = self.updateEdges( corporaObj, storedCorpora, ['Corpus'] )
        self.insertCorpora( corporaObj, overwrite=True )

    def updateCorpus( self, corpusObj, overwrite ):
        """updates or overwrite a corpus and associations"""
        if overwrite is True:
            self.insertCorpus( corpusObj, overwrite=True )
            return
        storedCorpus = self.loadCorpus( corpusObj['id'] )
        if storedCorpus is not None:
            corpusObj = self.updateEdges( corpusObj, storedCorpus, ['Document','NGram'] )
        self.insertCorpus( corpusObj, overwrite=True )

    def updateDocument( self, documentObj, overwrite ):
        """updates or overwrite a document and associations"""
        if overwrite is True:
            self.insertDocument( documentObj, overwrite=True )
            return
        storedDocument = self.loadDocument( documentObj['id'] )
        if storedDocument is not None:
            documentObj = self.updateEdges( documentObj, storedDocument, ['Corpus','NGram'] )
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
        _logger.debug("flushed ngram and cooc queues and indices")

    def _ngramQueue( self, id, ng ):
        """
        add a ngram to the queue and session index
        """
        self.ngramqueueindex += [id]
        self.ngramqueue += [(id, ng)]
        self.ngramindex += [id]
        queue = len( self.ngramqueue )
        return queue


    def updateNGram( self, ngObj, overwrite, docId, corpId ):
        """updates or overwrite a ngram and associations"""
        # overwrites while ngrams is not in self.ngramindex
        # if ngram is already into queue, will flush it first to permit incremental updates
        if ngObj['id'] in self.ngramqueueindex:
            self.flushNGramQueue()
        # else updates NGram
        storedNGram = self.loadNGram( ngObj['id'] )
        if storedNGram is not None:
            # if overwriting and NGram yet NOT in the current index
            if overwrite is True and ngObj['id'] not in self.ngramindex:
                # cleans current corpus edges
                if corpId in storedNGram['edges']['Corpus']:
                    del storedNGram['edges']['Corpus'][corpId]
                # NOTE : document edges are protected
                if docId in storedNGram['edges']['Document']:
                    del storedNGram['edges']['Document'][docId]
            ngObj = self.updateEdges( ngObj, storedNGram, ['Corpus','Document'] )
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


