# -*- coding: utf-8 -*-
from tinasoft.data import Handler
from bsddb3 import db
import thread
from threading import Thread
from os.path import dirname

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
        'locale'        : 'en_US.UTF-8',
        'format'        : 'jsonpickle',
        'dieOnError'    : False,
        'debug'         : False,
        'compression'   : None,
        'home'          : 'db',
        'prefix'        : {
            'Corpora':'Corpora::',
            'Corpus':'Corpus::',
            'Document':'Document::',
            'NGram':'NGram::',
            'Cooc':'Cooc::'
        },
    }

    def is_open(self):
        return self.__open

    def _init_db_environment(self, home, create=True):
        """modified from http://www.rdflib.net/"""
        envsetflags  = db.DB_CDB_ALLDB
        envflags = db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_THREAD | db.DB_INIT_TXN | db.DB_RECOVER
        db_env = db.DBEnv()
        db_env.set_cachesize(0, 1024*1024*100)  # 100 Mbytes

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
        self.lang,self.encoding = self.locale.split('.')
        dbname = None
        dbtype = db.DB_BTREE
        # auto-commit ensures that the open-call commits when transactions are enabled
        dbopenflags = db.DB_THREAD
        dbopenflags |= db.DB_AUTO_COMMIT
        dbmode = 0660
        dbsetflags   = 0
        # number of locks, lockers and objects
        self.__locks = 1000
        # when closing is True, no new transactions are allowed
        self.__closing = False
        # Each thread is responsible for a single transaction (included nested
        # ones) indexed by the thread id
        self.__dbTxn = {}
        # beware of the while self.__open in __sync_run() !
        self.__open = True
        if 'home' in opts:
            dir = opts['home']
        else:
            dir = self.options['home']
        self.db_env = db_env = self._init_db_environment(dir, create)
        self._db = db.DB(db_env)
        self._db.set_flags(dbsetflags)
        self._db.open(path, dbname, dbtype, dbopenflags|db.DB_CREATE, dbmode)
        self.__needs_sync = False
        # sync thread
        t = Thread(target=self.__sync_run)
        t.setDaemon(True)
        t.start()
        self.__sync_thread = t
        self.cursor = self._db.cursor
        #except db.DBError, dbe:
        #   raise Exception

    def __sync_run(self):
        """modified from http://www.rdflib.net/"""
        from time import sleep, time
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
            _logger.exception(e)

    def sync(self):
        """modified from http://www.rdflib.net/"""
        if self.__open:
            self._db.sync()

    def close(self, commit_pending_transaction=True):
        """
        Properly handles transactions explicitely (with parameter) or by default
        modified from http://www.rdflib.net/*
        """
        # when closing, no new transactions are allowed
        # problem is that a thread can already have passed the test and is
        # half-way through begin_txn when close is called...
        self.__closing = True
        if not self.is_open():
            return
        # this should close all existing transactions, not only by this thread,
        # uses the number of active transactions to sync on.
        if self.__dbTxn:
            # this will block for a while, depending on how long it takes
            # before the active transactions are committed/aborted
            # nactive = Number of transactions currently active.
            while self.db_env.txn_stat()['nactive'] > 0:
                active_threads = self.__dbTxn.keys()
                for t in active_threads:
                    if not commit_pending_transaction:
                        self.rollback(rollback_root=True)
                    else:
                        self.commit(commit_root=True)

                sleep(0.1)

        # there may still be open transactions
        self.__open = False
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
            raise Exception

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
                raise e
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
                raise e
        else:
            _logger.warning("No transaction to rollback")


    def encode( self, obj ):
        return self.serialize(obj)

    def decode( self, data ):
        return self.deserialize(data)

    def saferead( self, key, txn=None ):
        """gets a db entry or return None"""
        if isinstance(key, str) is False:
            key = str(key)
        try:
            return self._db.get(key, txn=txn)
        except db.DBNotFoundError, e1:
            _logger.error( "DBError exception during saferead() : " + e[1] )
            return None
        except db.DBKeyEmptyError, e2:
            _logger.error( "DBError exception during saferead() : " + e[1] )
            raise Exception

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
            raise Exception


    def safewrite( self, key, obj, overwrite=True, txn=None ):
        """
        wrapped in add()/transaction(), safely write an entry
        overwriting by default an existing key
        """
        if isinstance(key, str) is False:
            key = str(key)
        try:
            try:
                self._db.put(key, self.encode(obj), txn=txn)
            except db.DBKeyExistError, kee:
                _logger.debug( "existing key = "+ key )
                if overwrite is True:
                    self._db.delete( key, txn=txn )
                    self._db.put(key, self.encode(obj), txn=txn)
                    _logger.debug( "Overwriting an existing key = "+ key )
                    raise DBOverwrite()
        except DBOverwrite, dbo: pass
            #_logger.debug( "Overwriting an existing key = "+ key )

    def safewritemany( self, iter, target, overwrite=True, txn=None ):
        """writes a set of entries in the same transaction"""
        for pair in iter:
            self.safewrite( self.prefix[target]+pair[0], pair[1], overwrite, txn )

    def safedelete( self, key, txn=None ):
        """
        wrapped in remove()/transaction(), safely deletes an entry
        """
        if isinstance(key, str) is False:
            key = str(key)
        try:
            print "in delete"
            self._db.delete( key, txn=txn )
            print "out of delete"
        except db.DBNotFoundError, dbnf:
            _logger.debug( "delete failed, key not found = "+ key )
        except db.DBKeyEmptyError, dbke:
            _logger.debug( "delete failed, key is empty = "+ key )
        except db.DBError, dbe:
            _logger.debug( dbe )
            raise Exception()

    def add(self, key, obj, overwrite=True):
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
            _logger.error( "Got exception in add " )
            _logger.error( e )
            raise e

    def addmany(self, iter, target, overwrite=True):
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
            raise e

    def remove(self, id, deltype, delobj=None):
        """
        deletes a corpus and clean all the associations
        modified from http://www.rdflib.net/
        public method used in Engine
        """
        @transaction
        def _remove(self, id, deltype, delobj, txn=None):
            if delobj is None:
                delobj = self.saferead(self.prefix[deltype]+id, txn=txn)
                if delobj is not None:
                    # deletes an object
                    delobj = self.decode(delobj)
                else:
                    _logger.error("remove failed, key was not found")
                    _logger.error(self.prefix[deltype]+id)
                    return
            self.safedelete(self.prefix[deltype]+id, txn=txn)
            # deletes all mentions of this object in associated objects
            for type, assoc in delobj['edges'].iteritems():
                for associd in assoc.iterkeys():
                    # loads the associated object
                    assocobj = self.saferead(self.prefix[type]+associd, txn=txn)
                    if assocobj is not None:
                        # removes edges
                        assocobj = self.decode(assocobj)
                        if deltype not in assocobj['edges']:
                            _logger.debug("association type missing ("+deltype+") into obj "+associd)
                            continue
                        if id not in assocobj['edges'][deltype]:
                            _logger.debug("association "+type+" missing ("+id+") into obj "+associd)
                            continue
                        del assocobj['edges'][deltype][id]
                    else:
                        _logger.debug("Found inconsistent association, object not found :"+ self.prefix[type]+associd)
                        continue
                    self.safewrite(self.prefix[type]+associd, assocobj, True, txn)
        try:
            _remove(self, id, deltype, delobj)
        except Exception, e:
            _logger.error("Got exception in remove ")
            _logger.error(e)
            raise e

    def clear( self ):
        self._db.truncate()

class Engine(Backend):
    """
    bsddb Engine
    """
    def load(self, id, target, raw=False):
        read = self.saferead( self.prefix[target]+id )
        if read is not None:
            if raw is True:
                return read
            return self.decode(read)
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

    def insertMany(self, iter, target, overwrite=True):
        self.addmany(iter, target, overwrite)

    def insert( self, obj, target, id=None, overwrite=True ):
        if id is None:
            id = obj['id']
        self.add( self.prefix[target]+id, obj, overwrite )

    def insertCorpora(self, obj, id=None, overwrite=True ):
        self.insert( obj, 'Corpora', id, overwrite )

    def insertCorpus(self, obj, id=None, overwrite=True ):
        self.insert( obj, 'Corpus', id, overwrite )

    #def insertmanyCorpus(self, iter, overwrite=True ):
    #    for obj in iter:
    #        self.insertCorpus( obj, overwrite )

    def insertDocument(self, obj, id=None, overwrite=True ):
        """automatically removes text content before storing"""
        content = obj['content']
        obj['content'] = ""
        self.insert( obj, 'Document', id, overwrite  )
        obj['content'] = content

    #def insertmanyDocument(self, iter, overwrite=True ):
    #    # id, datestamp, blob
    #    for obj in iter:
    #        self.insertDocument( obj, overwrite )

    def insertNGram(self, obj, id=None, overwrite=True ):
        self.insert( obj, 'NGram', id, overwrite )

    def insertManyNGram(self, iter, overwrite=True ):
        self.insertMany( iter, 'NGram', overwrite )

    def insertCooc(self, obj, id, overwrite=True ):
        self.insert( obj, 'Cooc', id, overwrite )

    def insertAssoc(self, loadsource, sourcename, sourceid, insertsource,\
            loadtarget, targetname, targetid, inserttarget, occs, overwrite=True  ):
        """adds a unique edge from source to target et and increments occs=weight"""
        try:
            sourceobj = loadsource( sourceid )
            targetobj = loadtarget( targetid )
            # returns None if one of the objects does NOT exists
            if sourceobj is None or targetobj is None:
                raise DBInsertError()
        except DBInsertError, dbie:
            _logger.debug( "insertAssoc impossible" )
            return

        # inserts the edge in the source obj
        if targetname not in sourceobj['edges']:
            sourceobj['edges'][targetname]={}
        if targetid in sourceobj['edges'][targetname]:
            sourceobj['edges'][targetname][targetid]+=occs
        else:
            sourceobj['edges'][targetname][targetid]=occs
        insertsource( sourceobj, str(sourceobj['id']), overwrite )

        # in the target obj
        if sourcename not in targetobj['edges']:
            targetobj['edges'][sourcename]={}
        if sourceid in targetobj['edges'][sourcename]:
            targetobj['edges'][sourcename][sourceid]+=occs
        else:
            targetobj['edges'][sourcename][sourceid]=occs
        inserttarget( targetobj, str(targetobj['id']), overwrite )

    def insertAssocCorpus(self, corpusID, corporaID, occs=1 ):
        self.insertAssoc( self.loadCorpora, 'Corpora', corporaID,\
            self.insertCorpora, self.loadCorpus, 'Corpus', corpusID, \
            self.insertCorpus, occs )

    def insertAssocDocument(self, docID, corpusID, occs=1 ):
        self.insertAssoc( self.loadCorpus, 'Corpus', corpusID,\
            self.insertCorpus, self.loadDocument, 'Document', docID, \
            self.insertDocument, occs )

    def insertAssocNGramDocument(self, ngramID, docID, occs=1 ):
        self.insertAssoc( self.loadDocument, 'Document', docID,\
            self.insertDocument, self.loadNGram, 'NGram', ngramID, \
            self.insertNGram, occs )

    def insertAssocNGramCorpus(self, ngramID, corpID, occs=1 ):
        self.insertAssoc( self.loadCorpus, 'Corpus', corpID,\
            self.insertCorpus, self.loadNGram, 'NGram', ngramID, \
            self.insertNGram, occs )

    def selectCorpusCooc(self, corpusId):
        if isinstance(corpusId, str) is False:
            corpusId = str(corpusId)
        coocGen = self.select( self.prefix['Cooc']+corpusId )
        try:
            record = coocGen.next()
            while record:
                key = record[0].split('::')
                yield ( (key[2],key[3]), record[1])
                record = coocGen.next()
        except StopIteration, si: return


    def select( self, minkey, maxkey=None, raw=False ):
        cursor = self.safereadrange( minkey )
        record = cursor.first()
        while record:
            if maxkey is None:
                if record[0].startswith(minkey):
                    if raw is True:
                        yield record
                    else:
                        yield ( record[0], self.decode(record[1]))
            elif record[0] < maxkey:
                if raw is True:
                        yield record
                else:
                    yield ( record[0], self.decode(record[1]))
            else:
                return
            record = cursor.next()

    def deleteCorpus(self, id, delobj=None):
        """deletes a corpus and clean all the associations"""
        self.remove(id, 'Corpus', delobj)
