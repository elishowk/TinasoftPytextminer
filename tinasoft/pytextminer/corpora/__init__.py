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


    def __init__( self, reader, corpora, storage, index=None ):
        self.reader = reader
        self.corpora = corpora
        self.storage = storage
        self.index = index
        if self.index is not None:
            self.writer = index.getWriter()

    def _indexDocument( self, documentobj, overwrite ):
        if self.index is not None:
            if self.index.write(document, self.writer, overwrite) is None:
                _logger.debug("document content indexation skipped :" \
                    + str(document['id']))


    def walkFile( self, index, filters, ngramMin, ngramMax, stopwords, overwrite=False ):
        """Main parsing source file method"""
        self.duplicate = []
        fileGenerator = self.reader.parseFile()
        # 1st part = ngram extraction
        try:
            while 1:
                # document parsing, doc-corpus edge is written
                document, corpusNum = fileGenerator.next()

                # add Corpora-Corpus edges if possible
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)

                # indexation
                self._indexDocument( document, overwrite )

                # adds Corpus-Doc edge if possible
                self.reader.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)

                # checks document in storage
                if overwrite is False:
                    storedDoc = self.storage.loadDocument( document['id'] )
                    if storedDoc is not None:
                        # add the doc-corpus edge if possible
                        storedDoc.addEdge( 'Corpus', corpusNum, 1 )
                        # force update
                        self.storage.updateDocument( storedDoc, True )
                        _logger.warning( "Document is already stored %s : only updating its edges"%document['id'] )
                        # skip document
                        self.duplicate += [document]
                        continue

                # document's ngrams extraction
                self.extractNGrams( document, corpusNum,\
                ngramMin, ngramMax, filters, stopwords, overwrite )

        # Second part of file parsing = document graph updating
        except StopIteration, stop:
            # commit changes to indexer
            if index is not None:
                self.writer.commit()
            # inserts/updates corpus and corpora
            self.storage.updateCorpora( self.corpora, overwrite )
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            self.storage.flushNGramQueue()
            self.storage.ngramindex = []
            self.storage.commitAll()
            return True

    def extractNGrams( self, document, corpusNum, ngramMin,\
        ngramMax, filters, stopwords, overwrite ):
        """"Main NLP operations on a document THAT IS NOT ALREADY IN DATABASE"""
        _logger.debug( "%s is extracting document %s (overwrite=%s)"%(tokenizer.TreeBankWordTokenizer.__name__, document['id'], str(overwrite)) )
        # extract filtered ngrams
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            stopwords, ngramMin, ngramMax, filters )
        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            # init ngram's edges
            ng.addEdge( 'Corpus', corpusNum, 1 )
            ng.addEdge( 'Document', document['id'], docOccs )
            # updates doc-ngram and corpus-ngram edges
            document.addEdge( 'NGram', ng['id'], docOccs )
            self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            # queue the update of the ngram
            self.storage.updateNGram( ng, overwrite, document['id'], corpusNum )
        # creates or OVERWRITES document into storage
        self.storage.insertDocument( document, overwrite=True )
        self.storage.flushNGramQueue()
        self.storage.commitAll()
