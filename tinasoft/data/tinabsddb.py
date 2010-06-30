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

from bsddb3 import db

import thread
from threading import Thread

import pickle
from time import sleep, time

import logging
_logger = logging.getLogger('TinaAppLogger')

class DBOverwrite(db.DBError): pass
class DBInsertError(db.DBError): pass

class TransactionExpired(Exception): pass

# A transaction decorator for BDB
def transaction(f, name=None, **kwds):
    """

    from http://www.rdflib.net/
    """
    def wrapped(*args, **kwargs):
        bdb = args[0]
        retries = 1
        delay = 1
        e = None
        while retries > 0:
            kwargs['txn'] = bdb.begin_txn()
            try:
                result = f(*args, **kwargs)
                bdb.commit()
                # returns here when the transaction was successful
                return result
            except MemoryError, e:
                # Locks are leaking in this code or in BDB
                _logger.error(e)
                bdb.rollback()
                retries = 0
            except db.DBLockDeadlockError, e:
                _logger.error(e)
                bdb.rollback()
                sleep(0.1*delay)
                #delay = delay << 1
                retries -= 1
            except Exception, e:
                _logger.error(e)
                bdb.rollback()
                retries -= 1

        raise TransactionExpired("transaction failed after exception:" % str(e))

    wrapped.__doc__ = f.__doc__
    return wrapped


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
    }

    def is_open(self):
        return self.__open

    def _init_db_environment(self, home, create=True):
        """modified from http://www.rdflib.net/"""
        envsetflags  = db.DB_CDB_ALLDB
        envflags = db.DB_TXN_NOSYNC | db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_THREAD | db.DB_INIT_TXN | db.DB_RECOVER
        db_env = db.DBEnv()
        db_env.set_cachesize(0, 1024*1024*10)  # 100 Mbytes

        # enable deadlock-detection
        db_env.set_lk_detect(db.DB_LOCK_MAXLOCKS)

        # increase the number of locks,
        # this is correlated to the size that
        # can be added/removed with a single transaction
        db_env.set_lk_max_locks(self.__locks)
        db_env.set_lk_max_lockers(self.__locks)
        db_env.set_lk_max_objects(self.__locks)

        #db_env.set_lg_max(1024*1024)
        db_env.set_flags(envsetflags, 1)
        db_env.open(home, envflags | db.DB_CREATE,0)
        return db_env

    def __init__(self, path, create=True, **opts):
        self.path = path
        self.loadOptions(opts)
        #self.lang,self.encoding = self.locale.split('.')
        dbname = None
        dbtype = db.DB_BTREE
        # auto-commit ensures that the open-call commits when transactions are enabled
        dbopenflags = db.DB_THREAD
        dbopenflags |= db.DB_AUTO_COMMIT
        dbmode = 0660
        dbsetflags = 0
        # number of locks, lockers and objects
        self.__locks = 3000
        # when closing is True, no new transactions are allowed
        self.__closing = False
        # Each thread is responsible for a single transaction
        # (included nested ones) indexed by the thread id
        self.__dbTxn = {}
        # self.__open before __sync_run() !
        self.__open = True
        if 'home' in opts:
            dir = opts['home']
        else:
            dir = self.options['home']
        self.db_env = db_env = self._init_db_environment(dir, create)
        self._db = db.DB(db_env)
        self._db.set_flags(dbsetflags)
        #self._db.set_pagesize(65536)
        self._db.open(path, dbname, dbtype, dbopenflags|db.DB_CREATE, dbmode)

        self.__needs_sync = False
        # sync thread
        t = Thread(target=self.__sync_run)
        t.setDaemon(True)
        t.start()
        self.__sync_thread = t
        self.cursor = self._db.cursor
        self.MAX_INSERT_QUEUE = 500
        self.ngramqueue = []
        self.ngramqueueindex = []
        self.ngramindex = []
        self.coocqueue = []

    def __sync_run(self):
        """
        daemon thread responsible for periodically syncing to disk
        modified from http://www.rdflib.net/
        """
        try:
            min_seconds, max_seconds = 10, 300
            while self.__open:
                if self.__needs_sync:
                    t0 = t1 = time()
                    self.__needs_sync = False
                    while self.__open:
                        sleep(.1)
                        if self.__needs_sync:
                            t1 = time()
                            self.__needs_sync = False
                        # sync if
                        if time()-t1 > min_seconds or time()-t0 > max_seconds:
                            self.__needs_sync = False
                            #_logger.debug("sync")
                            self.sync()
                            break
                else:
                    sleep(1)
        except Exception, e:
            _logger.error(e)

    def sync(self):
        """modified from http://www.rdflib.net/"""
        if self.__open:
            #_logger.debug( self.db_env.memp_stat() )
            self._db.sync()

    def commitAll(self):
        # wait for transactions to finish
        self.closeAllTxn(commit_pending_transaction=True, commit_root=False)
        self.closeAllTxn(commit_pending_transaction=True, commit_root=True)
        #self.__needs_sync = True

    def closeAllTxn(self, commit_pending_transaction=True, commit_root=False):
        # this should close all existing transactions, not only by this thread,
        # uses the number of active transactions to sync on.
        if self.__dbTxn:
            # this will block for a while, depending on how long it takes
            # before the active transactions are committed/aborted
            # nactive = Number of transactions currently active.
            #_logger.debug( self.db_env.txn_stat() )
            while self.db_env.txn_stat()['nactive'] > 0:
                active_threads = self.__dbTxn.keys()
                for t in active_threads:
                    if not commit_pending_transaction:
                        self.rollback(rollback_root=True)
                    else:
                        self.commit(commit_root=commit_root)
                sleep(0.1)

    def close(self, commit_pending_transaction=True):
        """
        Properly handles transactions explicitely (with parameter) or by default
        modified from http://www.rdflib.net/*
        """
        # when closing, no new transactions are allowed
        # problem is that a thread can already have passed the test and is
        # half-way through begin_txn when close is called...
        self.flushQueues()
        self.__closing = True
        if not self.is_open():
            return
        self.commitAll()

        # there may still be open transactions
        self.__open = False
        # wait for all threads to finish
        self.__sync_thread.join()
        self._db.close()
        self.db_env.close()

    def destroy(self):
        """
        Destroys the underlying bsddb persistence for this store
        modified from http://www.rdflib.net/
        """
        # From bsddb docs:
        # A DB_ENV handle that has already been used to open an environment
        # should not be used to call the DB_ENV->remove function;
        # a new DB_ENV handle should be created for that purpose.
        self.close()
        db.DBEnv().remove(self.path,db.DB_FORCE)

    def __del__(self):
        """safely closes db and dbenv"""
        try:
            self.destroy()
        except db.DBError, e:
            _logger.error( "DBError exception during __del__() : " + e[1] )
            raise Exception(e)

    def begin_txn(self):
        """
        Starts a bsddb transaction. If the current thread already has a running
        transaction, a nested transaction with the first transaction for this
        thread as parent is started. See:
        http://pybsddb.sourceforge.net/ref/transapp/nested.html for more on
        nested transactions in BDB.
        modified from http://www.rdflib.net/
        """
        # A user should be able to wrap several operations in a transaction.
        # For example, two or more adds when adding a graph.
        # Each internal operation should be a transaction, e.g. an add
        # must be atomic and isolated. However, since add should handle
        # BDB exceptions (like deadlock), an internal transaction should
        # not fail the user transaction. Here, nested transactions are used
        # which have this property.
        txn = None
        try:
            if not thread.get_ident() in self.__dbTxn and self.is_open() and not self.__closing:
                self.__dbTxn[thread.get_ident()] = []
                # add the new transaction to the list of transactions
                txn = self.db_env.txn_begin()
                self.__dbTxn[thread.get_ident()].append(txn)
            else:
                # add a nested transaction with the top one as parent
                txn = self.db_env.txn_begin(self.__dbTxn[thread.get_ident()][0])
                self.__dbTxn[thread.get_ident()].append(txn)
        except Exception, e:
            _logger.debug( "begin_txn exception: " + e )
            if txn != None:
                txn.abort()

        # return the transaction handle
        return txn

    def commit(self, commit_root=False):
        """
        Bsddb tx objects cannot be reused after commit. Set rollback_root to
        true to commit all active transactions for the current thread.
        modified from http://www.rdflib.net/
        """
        if thread.get_ident() in self.__dbTxn and self.is_open():
            try:
                # when the root commits, all childs commit as well
                if commit_root == True:
                    self.__dbTxn[thread.get_ident()][0].commit(0)
                    # no more transactions, clean up
                    del self.__dbTxn[thread.get_ident()]
                else:
                    txn = self.__dbTxn[thread.get_ident()].pop()
                    #_logger.debug("committing")
                    #before = self.db_env.lock_stat()['nlocks']
                    txn.commit(0)
                    if len(self.__dbTxn[thread.get_ident()]) == 0:
                        del self.__dbTxn[thread.get_ident()]
            except IndexError, e:
                #The dbTxn for the current thread is removed to indicate that
                #there are no active transactions for the current thread.
                del self.__dbTxn[thread.get_ident()]
            except Exception, e:
                _logger.error( "Got exception in commit " + e )
                raise Exception(e)
        else:
            _logger.warning("No transaction to commit")

    def rollback(self, rollback_root=False):
        """
        Bsddb tx objects cannot be reused after commit. Set rollback_root to
        true to abort all active transactions for the current thread.
        modified from http://www.rdflib.net/
        """
        if thread.get_ident() in self.__dbTxn and self.is_open():
            _logger.debug("rolling back")
            try:
                if rollback_root == True:
                    # same as commit, when root aborts, all childs abort
                    self.__dbTxn[thread.get_ident()][0].abort()
                    del self.__dbTxn[thread.get_ident()]
                else:
                    txn = self.__dbTxn[thread.get_ident()].pop()
                    #before = self.db_env.lock_stat()['nlocks']
                    txn.abort()

                    if len(self.__dbTxn[thread.get_ident()]) == 0:
                        del self.__dbTxn[thread.get_ident()]
            except IndexError, e:
                #The dbTxn for the current thread is removed to indicate that
                #there are no active transactions for the current thread.
                del self.__dbTxn[thread.get_ident()]
            except Exception, e:
                _logger.error( "Got exception in rollback " + e )
                raise Exception(e)
        else:
            _logger.warning("No transaction to rollback")


    def pickle( self, obj ):
        return pickle.dumps(obj)

    def unpickle( self, data ):
        return pickle.loads(data)

    def saferead( self, key, txn=None ):
        """gets a db entry or return None"""
        if isinstance(key, str) is False:
            key = str(key)
        try:
            return self._db.get(key, txn=txn)
        except db.DBNotFoundError, e1:
            _logger.error( "DBNotFoundError exception : " + e1 )
            return None
        except db.DBKeyEmptyError, e2:
            _logger.error( "DBKeyEmptyError exception : " + e2 )
            raise Exception(e2)

    def safereadrange( self, smallestkey=None, txn=None ):
        """returns a cursor, optional smallest key"""
        if isinstance(smallestkey, str) is False:
            smallestkey = str(smallestkey)
        try:
            cur = self.cursor(txn=txn)
            if smallestkey is not None:
                cur.set_range( smallestkey )
            return cur
        except db.DBError, e:
            _logger.error( "DBError exception during safereadrange() : " + e[1] )
            raise Exception(e)

    def safewrite( self, key, obj, overwrite, txn=None ):
        """
        wrapped in add()/transaction(), safely write an entry
        overwriting by default an existing key
        """
        if isinstance(key, str) is False:
            key = str(key)
        obj = self.pickle(obj)
        try:
            self._db.put(key, obj, txn=txn)
        except db.DBKeyExistError, kee:
            if overwrite is True:
                self._db.delete( key, txn=txn )
                self._db.put(key, obj, txn=txn)
                return
            _logger.warning( "NOT overwriting key " + key )
        except db.DBLockDeadlockError, dead:
            _logger.error( dead )
            return


    def safewritemany( self, iter, target, overwrite, txn=None ):
        """
        writes a set of entries in the same transaction
        wrapped in addmany()/transaction(), safely inserts many entries within the same txn
        """
        for pair in iter:
            self.safewrite( self.prefix[target]+pair[0], pair[1], overwrite, txn )

    def safedelete( self, key, txn=None ):
        """
        wrapped in remove()/transaction(), safely deletes an entry
        """
        if isinstance(key, str) is False:
            key = str(key)
        try:
            #print "in delete"
            self._db.delete( key, txn=txn )
            #print "out of delete"
        except db.DBNotFoundError, dbnf:
            _logger.warning( "delete failed, key not found = "+ key )
        except db.DBKeyEmptyError, dbke:
            _logger.warning( "delete failed, key is empty = "+ key )
        except db.DBError, dbe:
            _logger.warning( dbe )
            raise Exception(dbe)

    def add(self, key, obj, overwrite):
        """
        modified from http://www.rdflib.net/
        public method used in Engine
        """
        @transaction
        def _add(self, key, obj, overwrite, txn=None):
            self.safewrite(key, obj, overwrite, txn)
        try:
            _add(self, key, obj, overwrite)
        except Exception, e:
            _logger.error( "Got exception in add()" )
            _logger.error( e )
            raise Exception()

    def addmany(self, iter, target, overwrite):
        """
        modified from http://www.rdflib.net/
        public method used in Engine
        """
        @transaction
        def _addmany(self, iter, target, overwrite, txn=None):
            self.safewritemany(iter, target, overwrite, txn)
        try:
            _addmany(self, iter, target, overwrite)
        except Exception, e:
            _logger.error( "Got exception in add " )
            _logger.error( e )
            raise Exception(e)

    def remove(self, id, deltype):
        """
        OBSOLETE
        deletes an object and clean all the associations
        modified from http://www.rdflib.net/
        public method used in Engine
        """
        @transaction
        def _remove(self, id, deltype, txn=None):
            #if delobj is None:
            #    delobj = self.saferead(self.prefix[deltype]+id, txn=txn)
            #    if delobj is not None:
            #        # deletes an object
            #        delobj = self.unpickle(delobj)
            #    else:
            #        _logger.warning("remove key was NOT found")
            #        _logger.warning(self.prefix[deltype]+id)
            #        return
            self.safedelete(self.prefix[deltype]+id, txn=txn)
            # deletes all mentions of this object in associated objects
            #for type, assoc in delobj['edges'].iteritems():
            #    for associd in assoc.iterkeys():
            #        # loads the associated object
            #        assocobj = self.saferead(self.prefix[type]+associd, txn=txn)
            #        if assocobj is not None:
            #            # removes edges
            #            assocobj = self.unpickle(assocobj)
            #            if deltype not in assocobj['edges']:
            #                _logger.warning("association "+deltype+" is missing ("+deltype+") into obj "+associd)
            #                continue
            #            if id not in assocobj['edges'][deltype]:
            #                _logger.warning("association to "+deltype+" "+id+" is missing  into obj "+associd)
            #                continue
            #            del assocobj['edges'][deltype][id]
            #        else:
            #            _logger.debug("Found inconsistent association, object not found :"+ self.prefix[type]+associd)
            #            continue
            #        self.safewrite(self.prefix[type]+associd, assocobj, True, txn)
        try:
            _remove(self, id, deltype)
        except Exception, e:
            _logger.error("Got exception in remove ")
            _logger.error(e)
            raise Exception()

    def clear( self ):
        self._db.truncate()

class Engine(Backend):
    """
    tinabsddb Engine
    """
    def delete(self, id, deltype):
        """deletes a object given its type and id"""
        self.remove(id, deltype)

    def load(self, id, target, raw=False):
        read = self.saferead( self.prefix[target]+id )
        if read is not None:
            if raw is True:
                return read
            return self.unpickle(read)
        else:
            return None

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
        self.addmany(iter, target, overwrite)

    def insert( self, obj, target, id=None, overwrite=False ):
        if id is None:
            id = obj['id']
        self.add( self.prefix[target]+id, obj, overwrite )

    def insertCorpora(self, obj, id=None, overwrite=False ):
        self.insert( obj, 'Corpora', id, overwrite )

    def insertCorpus(self, obj, id=None, overwrite=False ):
        self.insert( obj, 'Corpus', id, overwrite )

    def insertManyCorpus(self, iter, overwrite=False ):
        self.insertMany( iter, 'Corpus', overwrite )

    def insertDocument(self, obj, id=None, overwrite=False ):
        """automatically removes text content before storing"""
        self.insert( obj, 'Document', id, overwrite )

    def insertManyDocument(self, iter, overwrite=False ):
        self.insertMany( iter, 'Document', overwrite )

    def insertNGram(self, obj, id=None, overwrite=False ):
        self.insert( obj, 'NGram', id, overwrite )

    def insertManyNGram(self, iter, overwrite=False ):
        self.insertMany( iter, 'NGram', overwrite )

    def insertCooc(self, obj, id, overwrite=False ):
        self.insert( obj, 'Cooc', id, overwrite )

    def insertManyCooc( self, iter, overwrite=False ):
        self.insertMany( iter, 'Cooc', overwrite )

    def insertWhitelist(self, obj, id, overwrite=False ):
        self.insert( obj, 'Whitelist', id, overwrite )

    def insertManyWhitelist( self, iter, overwrite=False ):
        self.insertMany( iter, 'Whitelist', overwrite )

    def insertCluster(self, obj, id, overwrite=False ):
        self.insert( obj, 'Cluster', id, overwrite )

    def insertManyCluster( self, iter, overwrite=False ):
        self.insertMany( iter, 'Cluster', overwrite )

    def selectCorpusCooc(self, corpusId):
        """Yields a view on Cooc values given a period=corpus"""
        if isinstance(corpusId, str) is False:
            corpusId = str(corpusId)
        coocGen = self.select( self.prefix['Cooc']+corpusId )
        try:
            record = coocGen.next()
            while record:
                key = record[0].split('::')
                yield ( key[2], record[1])
                record = coocGen.next()
        except StopIteration, si: return


    def select( self, minkey, maxkey=None, raw=False ):
        """Yields raw or unpickled tuples from a range of key"""
        cursor = self.safereadrange( minkey )
        try:
            record = cursor.current()
        except db.DBInvalidArgError, dbinv:
            record = cursor.first()
        while record is not None:
            if maxkey is None:
                #_logger.debug(record[0]);
                if record[0].startswith(minkey):
                    if raw is True:
                        yield record
                    else:
                        yield ( record[0], self.unpickle(record[1]))
            elif record[0] < maxkey:
                if raw is True:
                        yield record
                else:
                    yield ( record[0], self.unpickle(record[1]))
            else:
                return
            record = cursor.next()
        cursor.close()

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
        self.ngramqueue += [[id, ng]]
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
        self.coocqueue += [[id, obj]]
        queue = len( self.coocqueue )
        if queue > self.MAX_INSERT_QUEUE:
            self.insertManyCooc( self.coocqueue, overwrite=overwrite )
            self.coocqueue = []
            return 0
        else:
            return queue

    def updateCooc( self, id, obj, overwrite ):
        """updates or overwrite Cooc row"""
        return self._coocQueue( id, obj, overwrite )


