# -*- coding: utf-8 -*-
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner, tokenizer, ngram, document, corpus
import logging
_logger = logging.getLogger('TinaAppLogger')

class Corpora(PyTextMiner):
    """
    Corpora is a work session
    Corpora contains a list of a corpus
    """

    def __init__(self, name, edges=None, **metas):
        # list of corpus id
        if edges is None:
            edges = {}
        if 'Corpus' not in edges:
            edges['Corpus'] = {}
        PyTextMiner.__init__(self, edges['Corpus'].keys(), name, name, edges=edges, **metas)

    def addEdge(self, type, key, value):
        #_logger.debug(key)
        if type == 'Corpus':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )


class Extractor():
    """corpora.Extractor is a source file importer = session = corpora"""


    def __init__( self, reader, corpora, storage ):
        self.reader = reader
        self.corpora = corpora
        self.storage = storage
        #self.graph = GraphStorage( storage )
        self.ngramqueue = []
        self.MAX_INSERT_QUEUE = 500

    def walkFile( self, index, filters, ngramMin, ngramMax, stopwords, overwrite=False ):
        """Main method for parsing tina csv source file"""
        if index is not None:
            writer = index.getWriter()
        fileGenerator = self.reader.parseFile( self.corpora )
        # first part of file parsing = ngram extraction
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                # document already exists into this corpus, continue
                #if self.updateDocument( document, corpusNum ) is False:
                #    continue
                # indexation
                if index is not None:
                    res = index.write(document, writer, overwrite)
                    if res is not None:
                        _logger.debug("document content indexation skipped :" \
                            + str(document['id']))
                # doc's ngrams extraction
                self.extractNGrams( document, corpusNum,\
                    ngramMin, ngramMax, filters, stopwords, overwrite)

        # Second part of file parsing = document graph updating
        except StopIteration, stop:
            if index is not None:
                # commit changes to indexer
                writer.commit()
            # updates corpora
            #self.corpora = self.reader.corpora
            self.storage.updateCorpora( self.reader.corpora, overwrite)
            # insert the new corpus
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            return

    def ngramQueue( self, id, obj, overwrite=True ):
        """
        Transaction queue grouping by self.MAX_INSERT_QUEUE
        overwrite should be True
        """
        updated_ng = self.storage.updateNGram( obj, overwrite=False )
        self.ngramqueue += [[id, updated_ng]]
        queue = len( self.ngramqueue )
        if queue > self.MAX_INSERT_QUEUE and self.storage is not None:
            self.storage.insertManyNGram( self.ngramqueue, overwrite=overwrite )
            _logger.debug( "insertManyNGram" )
            self.ngramqueue = []
            return 0
        else:
            return queue

    def extractNGrams(self, document, corpusNum, ngramMin,\
        ngramMax, filters, stopwords, overwrite):
        """"Main NLP operations for a document"""
        _logger.debug(tokenizer.TreeBankWordTokenizer.__name__+\
            " is working on document "+ document['id'])
        # get filtered ngrams
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            stopwords, ngramMin, ngramMax, filters )
        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            # increment ngram's edges
            ng['edges']['Corpus'][corpusNum] = 1
            ng['edges']['Document'][document['id']] = docOccs
            # increments doc-ngram edge
            document.addEdge( 'NGram', ng['id'], docOccs )
            # increments corpus-ngram edge
            self.reader.corpusDict[ corpusNum ].addEdge('NGram', ngid, 1)
            # prepares and queue insertion of ngram into storage
            queueSize = self.ngramQueue( ngid, ng, overwrite=overwrite )
        # insert doc in storage
        self.storage.updateDocument( document, overwrite )

