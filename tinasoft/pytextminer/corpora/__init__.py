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


    def __init__( self, reader, corpora ):
        self.reader = reader
        self.corpora = corpora

    def walkFile(self, storage, overwrite, index,\
        filters, ngramMin, ngramMax, stopwords):
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
                if index is not None:
                    res = index.write(document, writer, overwrite)
                    if res is not None:
                        _logger.debug("document will not be overwritten")
                ######### WARNING ngramindex is the same for all corpus
                document, ngrams = self.extractNGrams( storage, document, corpusNum,\
                    ngramMin, ngramMax, filters, stopwords)
                #ngramindex+=ngrams.keys()
                # TODO export docngrams (filtered)
                # Document-corpus Association are included in the object
                #docindex+=[document['id']]

        # Second part of file parsing
        except StopIteration, stop:
            if index is not None:
                # commit changes to indexer
                writer.commit()
            # insert or updates corpora
            self.corpora = self.reader.corpora
            storage.insertCorpora( self.corpora )
            # insert the new corpus
            for corpusNum in self.corpora['content']:
                # get the Corpus object and import
                #corpus = self.reader.corpusDict[ corpusNum ]
                #for docid in docindex:
                #    corpus.addEdge('Document', docid, 1)
                #corpus.addEdge('Corpora', corpora_id, 1)
                storage.insertCorpus(self.reader.corpusDict[ corpusNum ])
                #storage.insertAssocCorpus(self.reader.corpusDict[ corpusNum ]['id'], self.corpora['id'])
                yield self.reader.corpusDict[ corpusNum ], self.corpora
                del self.reader.corpusDict[ corpusNum ]
            return

    def extractNGrams(self, storage, document, corpusNum, ngramMin,\
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
            if storage is not None:
                storage.insertNGram( ng )
        # insert doc in storage
        if storage is not None:
            storage.insertDocument( document )
        # returns the ngram id index for the document
        return document, docngrams
