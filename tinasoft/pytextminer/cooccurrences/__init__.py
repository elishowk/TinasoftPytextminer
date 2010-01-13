# -*- coding: utf-8 -*-
__author__="Elias Showk"
from itertools import *
from multiprocessing import Process, Queue, Manager
import tinasoft

class Simple(tinasoft.TinaApp):
    """
    A homemade cooccurrences processor for TinaIndex()
    """

    def getCombinations(self):
        pass

    def getCorpusDocuments(self, corpus_id):
        corpus = self.storage.loadCorpus( corpus_id )
        if corpus is None:
            return None
        self.period_start = corpus[1]
        self.period_end = corpus[2]
        docList = self.storage.fetchCorpusDocumentID( corpus_id )

        ngrams = self.storage.fetchCorpusNGramID( corpus_id )
        documents = []
        # TODO : thread it !
        for docNum in docList:
            documents.append( self.storage.fetchDocumentNGramID( docNum )  )


class Multiprocessing(Simple):
    """
    Multiprocessing cooccurrences processor based on Simple()
    """
    def processDoc( self, result_queue, task_queue ):
        while task_queue.empty() is False:
            ( ngrams, ng1, ng2 ) = task_queue.get()
            print "starting processDoc"
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
            #threadDoc = [ Process(target=self.processDoc, args=( doc_result_queue, doc_task_queue )).start() for i in range(number_processes)]
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


