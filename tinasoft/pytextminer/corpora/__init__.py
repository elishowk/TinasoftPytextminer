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
        # Corpora can link only once to a Corpus
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

    def _indexDocument( self ): pass

    def walkFile( self, index, filters, ngramMin, ngramMax, stopwords, overwrite=False ):
        """Main method for parsing tina csv source file"""
        if index is not None:
            writer = index.getWriter()
        fileGenerator = self.reader.parseFile( )
        # 1st part = ngram extraction
        try:
            while 1:
                # document parsing
                document, corpusNum = fileGenerator.next()
                # indexation
                if index is not None:
                    res = index.write(document, writer, overwrite)
                    if res is not None:
                        _logger.debug("document content indexation skipped :" \
                            + str(document['id']))
                storedDoc = self.storage.loadDocument( document['id'] )
                if overwrite is True or storedDoc is None:
                    # writes corpus-document edges
                    self.reader.corpusDict[ corpusNum ]['content'] += [ document['id'] ]
                    self.reader.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)
                    # writes corpus-corpora edges : must occur only once
                    if corpusNum not in self.corpora['edges']['Corpus']:
                        self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                        self.corpora['content'] += [ corpusNum ]
                        self.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)
                    # document's ngrams extraction
                    self.extractNGrams( document, corpusNum,\
                        ngramMin, ngramMax, filters, stopwords, overwrite )

        # Second part of file parsing = document graph updating
        except StopIteration, stop:
            # commit changes to indexer
            if index is not None:
                writer.commit()
            # updates corpus and corpora
            self.storage.updateCorpora( self.corpora, overwrite )
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            return

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
            ng.addEdge( 'Corpus', corpusNum, 1 )
            ng.addEdge( 'Document', document['id'], docOccs )
            # increments doc-ngram edge
            document.addEdge( 'NGram', ng['id'], docOccs )
            # increments corpus-ngram edge
            self.reader.corpusDict[ corpusNum ].addEdge('NGram', ngid, 1)
            # prepares and queue insertion or update of the ngram into storage
            queueSize = self.storage.updateNGram( ng, overwrite )
        # update or create document into storage
        self.storage.updateDocument( document, overwrite )

