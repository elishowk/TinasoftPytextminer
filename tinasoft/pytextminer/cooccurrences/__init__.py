# -*- coding: utf-8 -*-
__author__="Elias Showk"

#from multiprocessing import Process, Queue, Manager
import tinasoft
from tinasoft.pytextminer import corpus
from datetime import date
#from threading import Thread
from numpy import *

import logging
_logger = logging.getLogger('TinaAppLogger')

class CoocMatrix():

    def __init__(self, size):
        self.reverse = {}
        self.lastindex = -1
        self.array = zeros((size,size),dtype=int8)

    def _getindex(self, key):
        if key in self.reverse:
            return self.reverse[key]
        else:
            self.lastindex += 1
            self.reverse[key] = self.lastindex
            return self.reverse[key]

    def get( self, key1, key2=None ):
        if key2 is None:
            return self.array[ self._getindex(key1), : ]
        else:
            return self.array[ self._getindex(key1), self._getindex(key2) ]

    def set( self, key1, key2, value=1 ):
        index1 = self._getindex(key1)
        index2 = self._getindex(key2)
        self.array[ index1, index2 ] += value



class MapReduce():
    """
    A homemade cooccurrences matrix calculator
    get a corpus number as input and returns a matrix's slice :
    { 'ngram_id' : {  dictionnary of all 'ngid2' : integer } }
    """
    def __init__(self, storage, whitelist, corpusid=None, filter=None):
        self.storage = storage
        self.filter = filter
        self.corpusid = corpusid
        self.corpus = self.storage.loadCorpus( self.corpusid )
        if self.corpus is None:
            raise Warning('Corpus not found')
        self.whitelist = whitelist
        self.matrix = CoocMatrix( len( self.whitelist.keys() ) )
        self.watcher = None

    def walkCorpus(self):
        """processes a list of documents into a corpus"""
        totaldocs = len(self.corpus['edges']['Document'].keys())
        doccount = 0
        for doc_id in self.corpus['edges']['Document']:
            obj = self.storage.loadDocument( doc_id )
            if obj is not None:
                mapgenerator = self.mapper(obj)
                try:
                    # documents loop
                    while mapgenerator:
                        term_map = mapgenerator.next()
                        self.reducer( term_map )
                except StopIteration, si: pass
            doccount += 1
            if doccount % 25 == 0:
                tinasoft.TinaApp.notify( None,
                    'tinasoft_runProcessCoocGraph_running_status',
                    'processed cooccurrences for %d of %d documents in period %s'%(doccount,totaldocs,self.corpusid)
                )

    def filterNGrams(self, ngrams):
        """
        Construct the filtered & white-listed map of an ngrams list
        """
        map = {}
        for ng in ngrams:
            obj = self.storage.loadNGram(ng)
            if obj is None:
                continue
            if obj['id'] not in self.whitelist:
                continue
            if self.filter is not None:
                passFilter = True
                for filt in self.filter:
                    passFilter &= filt.test(obj)
                if passFilter is False:
                    continue
            map[ng] = 1
        return map

    def mapper(self, doc):
        """
        generates a row for each ngram in the doc,
        cooccurrences to 1 to every other ngram in the doc
        """
        ngrams = doc['edges']['NGram'].keys()
        map = self.filterNGrams(ngrams)
        # map is a unity slice of the matrix
        for ng in map.keys():
            yield [ng,map]

    def reducer(self, term_map):
        """updates the cooc matrix"""

        ng1 = term_map[0]
        map = term_map[1]
        # key is the processed ngram
        #if ng1 not in self.matrix:
        #    self.matrix[ng1] = map
        #else:
        #    for ngi,ngicooc in map.iteritems():
        #        if ngi in self.matrix[ng1]:
        #            self.matrix[ng1][ngi] += ngicooc
        #        else:
        #            self.matrix[ng1][ngi] = ngicooc
        for ngi in map.iterkeys():
            self.matrix.set( ng1, ngi )
            #self.matrix[ng1][ngi] += ngicooc


    def writeMatrix(self, overwrite=True):
        """
        writes in the db rows of the matrix
        'Cooc::corpus::ngramid' => '{ 'ngx' : y, 'ngy': z }'
        """
        tinasoft.TinaApp.notify( None,
            'tinasoft_runProcessCoocGraph_running_status',
            'writing cooccurrences in database'
        )
        if self.corpusid is not None:
            key = self.corpusid+'::'
        else:
            return
        countng = 0
        totalrows = len(self.matrix.reverse.keys())
        #for ng in self.matrix:
        countcooc = 0
        #for destng in self.matrix[ng].iterkeys():
            #if self.matrix[ng][destng] > self.matrix[ng][ng]:
            #    _logger.error("inconsistent cooc : (diag %s) %d < %d (cooc %s)"%(ng,self.matrix[ng][ng],self.matrix[ng][destng],destng))
                #self.countDoc( ng )
        for ngi in self.matrix.reverse.iterkeys():
            row = {}
            for ngj in self.matrix.reverse.iterkeys():
                cooc = self.matrix.get( ngi, ngj )
                if cooc > 0:
                    countcooc += 1
                    row[ngj] = cooc
            self.storage.updateCooc( key+ngi, row, overwrite )
            countng += 1
            #if (countng % 100) == 0:
            #    tinasoft.TinaApp.notify( None,
            #        'tinasoft_runProcessCoocGraph_running_status',
            #        'processed %d / %d cooccurrences'%(countng,totalrows)
            #    )
        self.storage.flushCoocQueue()
        self.storage.commitAll()
        tinasoft.TinaApp.notify( None,
            'tinasoft_runProcessCoocGraph_running_status',
            'stored %d non-zero cooccurrences values'%(countcooc)
        )

    def readMatrix( self ):
        nodes = {}
        try:
            generator = self.storage.selectCorpusCooc( self.corpusid )
            while 1:
                id,row = generator.next()
                if self.whitelist is not None and id not in self.whitelist:
                    continue
                if id not in nodes:
                    nodes[id] = {}
                for ngram, cooc in row.iteritems():
                    if self.whitelist is not None and ngram not in self.whitelist:
                        continue
                    nodes[id][ngram] = cooc
        except StopIteration, si:
            return nodes

    def countDoc(self, ng):
        countdoc = 0
        ngobj = self.storage.loadNGram( ng )
        cursor = self.storage.safereadrange( "Document::" )
        record = cursor.next()
        while record[0].startswith("Document::"):
            docObj = self.storage.unpickle(record[1])
            if ng in docObj['edges']['NGram']:
                countdoc += 1
            record = cursor.next()
        _logger.error( "inconsistent NGram %s (with %d doc edges) appears in %d documents"%(ng,len(ngobj['edges']['Document'].keys()),countdoc) )

class Multiprocessing(MapReduce):
    """
    OBSOLETE
    Multiprocessing cooccurrences processor based on Simple()
    """
    def processDocs( self, result_queue, task_queue ):
        while task_queue.empty() is False:
            ( ngrams, ng1, ng2 ) = task_queue.get()
            print "starting processDocs"
            # put a new result
            if ng1 in ngrams and ng2 in ngrams:
                print "found a cooc for %s and %s" % (self.ng1, self.ng2)
                result_queue.put( 1 )
            else:
                result_queue.put( 0 )

    def processPair( self, result_queue, task_queue, number_processes=1 ):
        while task_queue.empty() is False:
            ( key, pair, documents, period_start, period_end ) = task_queue.get()
            print "starting processPair", pair
            matrixcell = 0
            doc_result_queue = Queue()
            doc_task_queue = Queue()
            for docngrams in documents:
                print docngrams
                print pair
                if pair[0] in docngrams and pair[1] in docngrams:
                    matrixcell += 1
                # add a task
                #doc_task_queue.put( ( docngrams, pair[0], pair[1] ) )
            # launch threads
            #threadDoc = [ Process(target=self.processDocs, args=( doc_result_queue, doc_task_queue )).start() for i in range(number_processes)]
            # get results
            #for i in range( len( threadDoc ) ):
                #matrixcell += doc_result_queue.get()
            print "matrixcell", matrixcell
            newRow = ( key, period_start, period_end, pair[0], pair[1], matrixcell )
            # put a new result
            result_queue.put( newRow )

    def analyseCorpus( self, store, corpus_id, number_processes ):
        documents = self.getCorpusDocuments( corpus_id )
        manager = Manager()
        docmanager = manager.list( documents )
        del documents
        del docList
        ngpairs = combinations( ngrams, 2 )
        del ngrams
        print "end of fetching ng pairs and doc ngrams"
        pair_task_queue = Queue()
        pair_result_queue = Queue()
        for pair in ngpairs:
            # add a task
            key = str(period_start)+"::"+str(period_end)+"::"+str(pair[0])+"::"+str(pair[1])
            pair_task_queue.put( ( key, pair, docmanager, period_start, period_end ) )
            break
        pair_task_queue.close()
        # launch threads
        del ngpairs
        threadPair = [Process(target=self.processPair, args=( pair_result_queue, pair_task_queue )).start() for i in range(number_processes)]
        # get results
        for i in range( len(threadPair) ):
            pairGet = pair_result_queue.get()
            print "got pair thread ", pairGet
            if pairGet[5] > 0:
                print "inserting a new Cooc row", pairGet
            #    store.insertCooc( pairGet )

    def analysePeriod( self, period_start, period_end ):
        pass


