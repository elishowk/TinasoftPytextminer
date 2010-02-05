# -*- coding: utf-8 -*-
__author__="Elias Showk"

#from multiprocessing import Process, Queue, Manager
from tinasoft.pytextminer import corpus
from datetime import date
from threading import Thread

import logging
_logger = logging.getLogger('TinaAppLogger')

class MapReduce():
    """
    A homemade cooccurrences matrix calculator
    get a corpus number as input
    and returns matrix =
    { 'ngram_id' :
        {
            'month1': {  dictionnary of all 'ngid2' : integer }
        }
    }
    """
    def __init__(self, storage, corpus=None, filter=None):
        self.storage = storage
        self.filter = filter
        self.corpus=corpus
        self.matrix = {}
        self.processedDocs = []

    def walkCorpus(self):
        """another way to generate a cooc matrix"""
        corpus = self.storage.loadCorpus( self.corpus )
        if corpus is not None:
            docs = corpus['edges']['Document']
            _logger.debug( "Documents in the corpus = "+ str(len( docs.keys())))
            self.mapReduceDocs( docs )

    def mapReduceDocs(self, docs):
        """processes a list document sent by the ngram"""
        for doc_id in docs.iterkeys():
            if doc_id not in self.processedDocs:
                obj = self.storage.loadDocument( doc_id )
                if obj is not None:
                    # TODO encapsulate doc date parsing
                    ( day, month, year ) = obj['date'].split('/')
                    monthyear = month+year
                    mapgenerator = self.mapper(obj)
                    try:
                        # ngrams loop
                        termDict = mapgenerator.next()
                        while termDict:
                            self.reducer( monthyear, termDict )
                            termDict = mapgenerator.next()
                    except StopIteration, si: pass
                self.processedDocs += [doc_id]
            else:
                _logger.debug( "Already processed Doc = "+ str(doc_id) )


    def filterNGrams(self, ngrams):
        map = {}
        for ng in ngrams:
            obj = self.storage.loadNGram(ng)
            if obj is not None:
                if self.filter is not None:
                    passFilter = True
                    for filt in self.filter:
                        passFilter &= filt.test(obj)
                    if passFilter is True:
                        map[ng]=1
                else:
                    map[ng]=1
        _logger.debug( "Ngrams passing filters = "+ str(len( map.keys() )) )
        return map

    def mapper(self, doc):
        """
        generates a row for each ngram in the doc,
        cooccurrences to 1 to every other ngram in the doc
        """
        ngrams = doc['edges']['NGram'].keys()
        _logger.debug( "Ngrams found in doc = "+ str(len(ngrams) ))
        map = self.filterNGrams(ngrams)
        # map is a dict of each ngrams in the document
        # associated with a 1 cooc score with every other ngrams
        for ng in ngrams:
            if ng in map.keys():
                yield { ng : map }

    def reducer(self, month, term):
        """updates the cooc matrix"""
        key = term.keys()
        row = term[key[0]]
        # key[0] is the processed ngram
        if key[0] not in self.matrix:
            self.matrix[key[0]] = {}
        if month not in self.matrix[key[0]]:
            self.matrix[key[0]][month] = row
        else:
            for assocterm in row.iterkeys():
                if assocterm in self.matrix[key[0]][month]:
                    #_logger.debug( "=== Incrementing cooc value for "+ assocterm )
                    self.matrix[key[0]][month][assocterm] += row[assocterm]
                else:
                    self.matrix[key[0]][month][assocterm] = row[assocterm]

    def writeMatrix(self):
        """
        writes in the db rows of the matrix
        'Cooc::corpus::ngramid::yearmonth' => '{ 'ngx' : y, 'ngy': z }'
        """
        if self.corpus is not None:
            key = self.corpus+'::'
        else:
            return self.matrix
        count = 0
        countng = 0
        for ng in self.matrix:
            countng+=1
            #count = 0
            for month in self.matrix[ng]:
                count += 1
                self.storage.insertCooc(self.matrix[ng][month],\
                    key+ng+'::'+month)
            #_logger.debug( "Wrote total months = "+ str(count) )
        _logger.debug( "total ngrams analysed = "+ str(countng) )
        _logger.debug( "total cooc rows written = "+ str(count) )


class Multiprocessing(MapReduce):
    """
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


