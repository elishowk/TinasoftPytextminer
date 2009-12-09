# -*- coding: utf-8 -*-
__author__="Elias Showk"
from itertools import *
from multiprocessing import Process, Queue



class ThreadedAnalysis():
    """
    A homemade coword analyzer
    """
    def process( self, result_queue, task_queue ):
        ( ngrams, ng1, ng2 ) = task_queue.get()
        if ng1 in ngrams and ng2 in ngrams:
            print "found a cooc for %s and %s" % (self.ng1, self.ng2)
            result_queue.put( 1 )

    def analyseCorpus( self, store, corpusID ):
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
        for pair in ngpairs:
            key = str(period_start)+"::"+str(period_end)+"::"+str(pair[0])+"::"+str(pair[1])
            matrixcell = 0
            result_queue = Queue()
            task_queue = Queue()
            for docngrams in documents:
                # launch thread
                task_queue.put( ( docngrams, pair[0], pair[1] ) )
                threadDoc = Process(target=self.process, args=( result_queue, task_queue ))
                threadDoc.start()
                matrixcell += result_queue.get()
                #print "started %s" % key
            # wait all pairs threads to resume 
            #task_queue.join()
            if matrixcell > 0:
                newRow = ( key, period_start, period_end, pair[0], pair[1], matrixcell )
                store.insertCooc( newRow )
        #return self.matrix

    def analysePeriod( self, period_start, period_end ):
        pass
    
class SimpleAnalysis():
    def run(self, docngrams, ng1, ng2):
        if ng1 in docngrams and ng2 in docngrams:
            return 1
        else:
            return 0

    def processCorpus( self, store, corpusID ):
        corpus = store.loadCorpus( corpusID )
        if corpus is None:
            return None
        period_start = corpus[1]
        period_end = corpus[2]
        ngrams = store.fetchCorpusNGramID( corpusID )
        docList = store.fetchCorpusDocumentID( corpusID )
        documents = []
        for docNum in docList:
            documents.append( store.fetchDocumentNGramID( docNum )  )
        ngpairs = combinations( ngrams, 2 )
        matrix = {}
        for pair in ngpairs:
            key = str(period_start)+"::"+str(period_end)+"::"+str(pair[0])+"::"+str(pair[1])
            counter = 0
            for docngrams in documents:
                if self.run( docngrams, pair[0], pair[1] ) == 1:
                    counter = counter + 1
            if counter > 0:
                print matrix[key]
                matrix[ key ] = ( key, period_start, period_end, pair[0], pair[1], counter )
        return matrix

    def processPeriod( self, period_start, period_end ):
        pass

class ThreadedDocumentJob():
    """
    A homemade coword analyzer
    """
    def __init__(self, ngrams, ng1, ng2, matrixcell):
        threading.Thread.__init__(self)
        self.ngrams = ngrams
        self.ng1 = ng1
        self.ng2 = ng2
        self.matrixcell = matrixcell

    def run(self):
        if self.ng1 in self.ngrams and self.ng2 in self.ngrams:
            print "found a cooc for %s and %s" % (self.ng1, self.ng2)
            self.matrixcell += 1
