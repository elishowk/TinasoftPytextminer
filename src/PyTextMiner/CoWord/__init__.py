# -*- coding: utf-8 -*-
__author__="Elias Showk"
from itertools import *
from multiprocessing import Process, Queue



class ThreadedAnalysis():
    """
    A homemade coword analyzer
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

    def processPair( self, result_queue, task_queue, number_processes ):
        while task_queue.empty() is False:
            ( key, pair, documents, period_start, period_end ) = task_queue.get()
            print "starting processPair", pair
            matrixcell = 0
            doc_result_queue = Queue()
            doc_task_queue = Queue()
            for docngrams in documents:
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

    def analyseCorpus( self, store, corpusID, number_processes ):
        corpus = store.loadCorpus( corpusID )
        if corpus is None:
            return None
        period_start = corpus[1]
        period_end = corpus[2]
        ngrams = store.fetchCorpusNGramID( corpusID )
        docList = store.fetchCorpusDocumentID( corpusID )
        documents = []
        # TODO : thread it !
        for docNum in docList:
            documents.append( store.fetchDocumentNGramID( docNum )  )
        ngpairs = combinations( ngrams, 2 )
        print "end of fetching ng pairs and doc ngrams"
        pair_task_queue = Queue()
        pair_result_queue = Queue()
        for pair in ngpairs:
            # add a task
            key = str(period_start)+"::"+str(period_end)+"::"+str(pair[0])+"::"+str(pair[1])
            print key
            pair_task_queue.put( ( key, pair, documents, period_start, period_end ) )
        # launch threads
        threadPair = [Process(target=self.processPair, args=( pair_result_queue, pair_task_queue )).start() for i in range(number_processes)]
        # get results
        for i in range( len(threadPair) ):
            pairGet = pair_result_queue.get()
            print "pair thread get ", pairGet
            if pairGet[5] > 0:
                print "inserting a new Cooc row", pairGet
            #    store.insertCooc( pairGet )

    def analysePeriod( self, period_start, period_end ):
        pass
    

