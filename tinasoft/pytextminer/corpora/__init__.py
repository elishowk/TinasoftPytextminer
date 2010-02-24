# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner, tokenizer
import logging
_logger = logging.getLogger('TinaAppLogger')

class Corpora(PyTextMiner):
    """
    Corpora is a work session
    Corpora contains a list of a corpus
    """

    def __init__(self, name, corpuslist=None, **metas):
        # list of corpus id
        if corpuslist is None:
            corpuslist = []
        PyTextMiner.__init__(self, corpuslist, name, name, edges={ 'Corpus': {} }, **metas)

class Extractor():
    """corpora.Extractor is a source file importer = session = corpora"""


    def __init__( self, reader, corpora, storage ):
        self.reader = reader
        self.corpora = corpora
        self.storage = storage
        self.ngramqueue = []
        self.MAX_INSERT_QUEUE = 500

    def walkFile( self, overwrite, index, filters, ngramMin, ngramMax, stopwords ):
        """Main method for parsing tina csv source file"""
        if index is not None:
            writer = index.getWriter()
        fileGenerator = self.reader.parseFile( self.corpora )
        #docindex=[]
        #ngramindex=[]
        # fisrt part of file parsing
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                storedDoc = self.storage.loadDocument(document['id'])
                if storedDoc is not None and overwrite is False:
                # document already exists
                    self.updateDocument( document, storedDoc, corpusNum )
                    continue
                # indexation
                if index is not None:
                    res = index.write(document, writer, overwrite)
                    if res is not None:
                        _logger.debug("document will not be overwritten")
                # extraction
                document, ngrams = self.extractNGrams( document, corpusNum,\
                    ngramMin, ngramMax, filters, stopwords)

        # Second part of file parsing
        except StopIteration, stop:
            if index is not None:
                # commit changes to indexer
                writer.commit()
            # updates corpora
            self.corpora = self.reader.corpora
            self.storage.insertCorpora( self.corpora )
            # insert the new corpus
            for corpusObj in self.reader.corpusDict.values():
                self.storage.insertCorpus(corpusObj)
                yield corpusObj, self.corpora['id']
                del self.reader.corpusDict[ corpusNum ]
            return

    def updateDocument( self, document, storedDoc, corpusNum ):
         _logger.debug("duplicate document, skipping extraction :" + str(document['id']))
        storedDoc

    def ngramQueue( self, id, obj ):
        """Transaction queue grouping by self.MAX_INSERT_QUEUE the number of NGram insertion in db"""
        self.ngramqueue += [[id, obj]]
        queue = len( self.ngramqueue )
        if queue > self.MAX_INSERT_QUEUE and self.storage is not None:
            self.storage.insertManyNGram( self.ngramqueue )
            self.ngramqueue = []
            return 0
        else:
            return queue

    def extractNGrams(self, document, corpusNum, ngramMin,\
        ngramMax, filters, stopwords):
        """"Main NLP operation for a document"""
        _logger.debug(tokenizer.TreeBankWordTokenizer.__name__+\
            " is working on document "+ document['id'])
        # get filtered ngrams
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            stopwords, ngramMin, ngramMax, filters )
        for ngid, ng in docngrams.iteritems():
            # save document occurrences and delete it
            docOccs = ng['occs']
            del ng['occs']
            # increments document-ngram edges
            document.addEdge( 'NGram', ng['id'], docOccs )
            ng.addEdge( 'Document', document['id'], docOccs )
            # increments ng-corpus edge
            ng.addEdge( 'Corpus', corpusNum, 1 )
            self.reader.corpusDict[ corpusNum ].addEdge('NGram', ngid, 1)
            self.ngramQueue( ngid, ng )
        # insert doc in storage
        if self.storage is not None:
            self.storage.insertDocument( document )
        # returns the ngram id index for the document
        return document, docngrams
