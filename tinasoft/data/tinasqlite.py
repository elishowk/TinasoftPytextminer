#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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

__author__="elishowk@nonutc.fr"

from tinasoft.data import Handler
from tinasoft.pytextminer import ngram

import sqlite3

from os.path import exists
from os.path import join
from os import makedirs

import cPickle as pickle

import logging
_logger = logging.getLogger('TinaAppLogger')

sqlite3.enable_callback_tracebacks(True)

class Backend(Handler):
    """
    Low-Level sqlite3 database backend
    """
    options = {
        'home' : 'db',
        'drop_tables': False
    }

    def __init__(self, path, **opts):
        """loads options, connect or create db"""
        self.loadOptions(opts)
        self.path = path
        if not exists(self.home):
            makedirs(self.home)
        self._connect()
        ### empty database if needed
        if self.drop_tables is True:
            _logger.debug("will drop all tables of db %s"%self.path)
            self._drop_tables()
        ### create  tables if needed
        self._create_tables()
        ### checks success
        if self.is_open() is False:
            raise Exception("Unable to open a tinasqlite database")

    def is_open(self):
        return self.__open

    def _drop_tables(self):
        try:
            with self._db:
                for tabname in self.tables:
                    sql = "DROP TABLE IF EXISTS %s"%tabname
                    self._db.execute(sql)
        except sqlite3.Error, _exc:
            raise Exception( "error on _drop_tables(): %s"%_exc )

    def _connect(self):
        """connection method, need to have self.home directory created"""
        try:
            self._db = sqlite3.connect(join(self.home,self.path), isolation_level=None)
            # row factory provides named columns
            self._db.row_factory = sqlite3.Row
            self.__open = True
        except Exception, connect_exc:
            _logger.error("_connect() error : %s"%connect_exc)
            raise Exception(connect_exc)

    def _create_tables(self):
        try:
            with self._db:
                self._db.execute("PRAGMA SYNCHRONOUS=0;")
                for tabname in self.tables:
                    sql = "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(256) PRIMARY KEY, pickle BLOB)"%tabname
                    self._db.execute(sql)
        except sqlite3.Error, _exc:
            raise Exception( "error on _create_tables(): %s"%_exc )

    def close(self):
        """
        Properly handles transactions explicitely (with parameter) or by default

        """
        if not self.is_open():
            return
        self._db.close()
        self.__open = False

    def commit(self):
        """
        commit or rollback
        """
        try:
            self._db.commit()
        except Exception, commitexc:
            self.rollback()
            raise Exception("cannot commit() : %s"%commitexc)

    def rollback(self):
        """
        rollbacks
        """
        self._db.rollback()

    def pickle( self, obj ):
        return pickle.dumps(obj)

    def unpickle( self, data ):
        if type(data) != str:
            data = str(data)
        return pickle.loads(data)

    def saferead(self, tabname, key):
        """returns ONE db value or return None"""
        if tabname not in self.tables:
            return
        if type(key) != str:
            key = str(key) 
        cur = self._db.cursor()
        cur.execute("select pickle from %s where id=?"%tabname, (key,))
        row = cur.fetchone()
        if row is not None:
            return row["pickle"]
        else:
            return None

    def safereadall(self, tabname):
        """returns a list of records from an entire table"""
        if tabname not in self.tables:
            return
        cur = self._db.cursor()
        cur.execute("select id, pickle from %s"%tabname)
        return cur.fetchall()

    def safereadrange(self, tabname):
        """returns a cursor of a whole table"""
        if tabname not in self.tables:
            return
        cur = self._db.cursor()
        cur.execute("select id, pickle from %s"%tabname)
        next_val = cur.fetchone()
        while next_val is not None:
            yield next_val
            next_val = cur.fetchone()
        return

    def safewrite( self, tabname, list_of_tuples ):
        """
        Pickles a list of tuples
        then execute many inserts of this transformed list of tuples
        """
        if tabname not in self.tables:
            return
        pickled_list = [(key,buffer(self.pickle(obj))) for (key, obj) in list_of_tuples]
        try:
            with self._db:
                self._db.executemany("insert or replace into %s (id, pickle) values (?,?)"%tabname, pickled_list)
                self._db.commit()
        except sqlite3.Error, insert_exc:
            raise Exception( "error on safewrite(), table %s : %s"%(tabname,insert_exc) )

    def safedelete(self, tabname, key):
        """
        DELETE an object from database within a transaction
        """
        if tabname not in self.tables:
            return
        try:
            with self._db:
                self._db.execute("DELETE FROM %s WHERE id=?"%tabname, (key,))
                self._db.commit()
        except sqlite3.Error, del_exc:
            raise Exception( "exception on safedelete(), table %s : %s"%(tabname,del_exc) )


class Engine(Backend):
    """
    High level database Engine of Pytextminer
    """
    options = {
        'home' : 'db',
        'drop_tables': False,
        'tables' : [
            'Corpora',
            'Corpus',
            'Document',
            'NGram',
            'Whitelist',
            'Cluster',
            'GraphPreprocessNGram',
            'GraphPreprocessDocument'
        ],
    }

    def __del__(self):
        """safely closes db and dbenv"""
        #self.flushQueues()
        self.close()

    def load(self, id, target, raw=False):
        read = self.saferead( target, id )
        if read is not None:
            if raw is True:
                return read
            return self.unpickle(str(read))
        else:
            return None

    def loadAll(self, target, raw=False):
        """
        returns a generator of tuples (id, pickled_obj)
        """
        allrecords = self.safereadall(target)
        for record in allrecords:
            if not raw:
                yield ( record["id"], self.unpickle(str(record["pickle"])))
            else:
                yield record

    def loadCorpora(self, id, raw=False ):
        return self.load(id, 'Corpora', raw)

    def loadCorpus(self, id, raw=False ):
        return self.load(id, 'Corpus', raw)

    def loadDocument(self, id, raw=False ):
        return self.load(id, 'Document', raw)

    def loadNGram(self, id, raw=False ):
        return self.load(id, 'NGram', raw)

    def loadGraphPreprocess(self, id, category, raw=False ):
        return self.load(id, 'GraphPreprocess'+category, raw)

    def loadWhitelist(self, id, raw=False):
        return self.load(id, 'Whitelist', raw)

    def loadCluster(self, id, raw=False):
        return self.load(id, 'Cluster', raw)

    def insertMany(self, iter, target):
        """insert many objects from a list"""
        if len(iter) != 0:
            return self.safewrite(target, iter)

    def insert( self, obj, target, id=None ):
        """insert one object given its type"""
        if id is None:
            id = obj['id']
        return self.safewrite( target, [(id, obj)] )

    def insertCorpora(self, obj, id=None ):
        return self.insert( obj, 'Corpora', id )

    def insertCorpus(self, obj, id=None ):
        return self.insert( obj, 'Corpus', id )

    def insertManyCorpus(self, iter ):
        return self.insertMany( iter, 'Corpus' )

    def insertDocument(self, obj, id=None ):
        return self.insert( obj, 'Document', id )

    def insertManyDocument(self, iter):
        return self.insertMany( iter, 'Document' )

    def insertNGram(self, obj, id=None ):
        return self.insert( obj, 'NGram', id )

    def insertManyNGram(self, iter ):
        return self.insertMany( iter, 'NGram' )

    def insertGraphPreprocess(self, obj, id, type ):
        """
        @id "corpus_id::node_id"
        @obj graph row dict
        """
        return self.insert( obj, 'GraphPreprocess'+type, id )

    def insertManyGraphPreprocess( self, iter, type ):
        return self.insertMany( iter, 'GraphPreprocess'+type )

    def insertWhitelist(self, obj, id ):
        return self.insert( obj, 'Whitelist', id )

    def insertManyWhitelist( self, iter ):
        return self.insertMany( iter, 'Whitelist' )

    def insertCluster(self, obj, id ):
        return self.insert( obj, 'Cluster', id )

    def insertManyCluster( self, iter ):
        return self.insertMany( iter, 'Cluster' )

    def _neighboursUpdate(self, obj, target):
        """
        updates EXISTING neighbours symmetric edges and removes zero weighted edges
        """
        for category in obj['edges'].keys():
            if category == target: continue
            for neighbourid in obj['edges'][category].keys():
                neighbourobj = self.load(neighbourid, category)
                if neighbourobj is not None:
                    if obj['edges'][category][neighbourid] <= 0:
                        #del obj['edges'][category][neighbourid]
                        if obj['id'] in neighbourobj['edges'][target]:
                            del neighbourobj['edges'][target][obj['id']]
                    else:
                        neighbourobj['edges'][target][obj['id']] = obj['edges'][category][neighbourid]
                    self.insert(neighbourobj, category)
                else:
                    _logger.warning("neighbour %s for node %s is missing in database"%(neighbourid,obj.id))

    def update( self, obj, target, redondantupdate=False ):
        """updates an object and its edges"""
        stored = self.load( obj['id'], target )
        if stored is not None:
            stored.updateObject(obj)
            obj = stored
        if redondantupdate is True:
            self._neighboursUpdate(obj, target)
        # storage object in case it's a Document otherwise it's not used
        obj._cleanEdges(storage=self)
        self.insert( obj, target )

    def updateWhitelist( self, obj, redondantupdate=False ):
        """updates a Whitelist and associations"""
        self.update( obj, 'Whitelist', redondantupdate )

    def updateCluster( self, obj, redondantupdate=False ):
        """updates a Cluster and associations"""
        self.update( obj, 'Cluster', redondantupdate )

    def updateCorpora( self, obj, redondantupdate=False ):
        """updates a Corpora and associations"""
        self.update( obj, 'Corpora', redondantupdate )

    def updateCorpus( self, obj, redondantupdate=False ):
        """updates a Corpus and associations"""
        self.update( obj, 'Corpus', redondantupdate )

    def updateDocument( self, obj, redondantupdate=False ):
        """updates a Document and associations"""
        self.update( obj, 'Document', redondantupdate )

    def updateNGram( self, obj, redondantupdate=False ):
        """
        updates a ngram and associations
        """
        self.update( obj, 'NGram', redondantupdate )

    def selectCorpusGraphPreprocess(self, corpusId, tabname):
        """
        Yields tuples (node_id, db_row)
        for a given a corpus id
        """
        rowGen = self.select( tabname, corpusId )
        try:
            record = rowGen.next()
            while record:
                # separate CORPUSID::OBJID
                key = record[0].split('::')
                yield (key[1], record[1])
                record = rowGen.next()
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

    def delete(self, id, target, redondantupdate=False):
        """
        deletes a object given its type and id
        """
        if redondantupdate is True:
            obj = self.load(id, target)
            if obj is None: return
            for cat in obj.edges.keys():
                for neighbour_id in obj.edges[cat].keys():
                    neighbour_obj = self.load(neighbour_id, cat)
                    if neighbour_obj is None: continue
                    if id in neighbour_obj.edges[target]:
                        del neighbour_obj.edges[target][id]
                    self.insert(neighbour_obj, cat)
        self.safedelete(target, id)

    def deleteNGramForm(self, form, ngid, is_keyword, docid=None):
        """
        removes a NGram's form if every Documents it's linked to
        """
        # updates the NGram first
        ng = self.loadNGram(ngid)
        if docid is None:
            doclist = ng['edges']['Document'].keys()
            ng.deleteForm(form, is_keyword)
            if len(ng['edges']['label'].keys())==0:
                _logger.warning("deleting NGram %s"%ng.id)
                self.delete(ngid, "NGram", True)
                return
            else:
                self.insertNGram(ng)
        else:
            doclist = [docid]
        doc_count = 0
        for doc_id in doclist:
            doc = self.loadDocument(doc_id)
            doc.deleteNGramForm(form, ngid, is_keyword)
            # propagates decremented edges to its neighbours
            self._neighboursUpdate(doc, 'Document')
            doc._cleanEdges(self)
            self.insertDocument(doc)
            doc_count += 1
            #yield None
        yield [form, doc_count]
        return

    def addNGramForm(self, form, keyword_doc_id, is_keyword):
        """
        adds a NGram as a form to all the dataset's Documents
        """
        new_ngram = ngram.NGram(form.split(" "), label=form)
        # decrements label count
        new_ngram['edges']['label'][new_ngram['edges']['label'].keys()[0]] -= 1
        # checks existing NGram with the same id
        stored_ngram = self.loadNGram(new_ngram.id)
        if stored_ngram is not None:
            # only updates form attributes
            new_ngram = stored_ngram

        new_ngram._overwriteEdge("keyword", form, True)
        # pre-saves the NGram, in order to permit further redondant updates
        self.insertNGram(new_ngram, new_ngram.id)
        # first and only dataset
        dataset_gen = self.loadAll("Corpora")
        (dataset_id, dataset) = dataset_gen.next()
        doc_count = 0

        # walks through all documents
        for corp_id in dataset['edges']['Corpus'].keys():
            corpus_obj = self.loadCorpus(corp_id)
            for docid in corpus_obj['edges']['Document'].keys():
                total_occs = 0
                doc = self.loadDocument(docid)
                if docid != keyword_doc_id:
                    total_occs += doc.addNGramForm(new_ngram, self, False)
                else:
                    # if is_keyword is True, forces NGram-Document linking
                    total_occs += doc.addNGramForm(new_ngram, self, is_keyword)
                if total_occs > 0:
                    # saves the modified document
                    self.insertDocument(doc)
                    doc_count += 1
                #yield None
        yield [form, doc_count]
        return
