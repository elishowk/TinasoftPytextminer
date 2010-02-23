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

    def __init__(self, name, corpora=None, **metas):
        # list of corpus id
        if corpora is None:
            corpora = {}
        PyTextMiner.__init__(self, corpora, name, name, edges={ 'Corpus': corpora }, **metas)

class Extractor():
    """corpora.Extractor is a source file importer = session = corpora"""
    def walkFile(self, storage, reader, corpora_id, overwrite, index,\
        filters, ngramMin, ngramMax, stopwords):
        """Main method for parsing tina csv source file"""
        if index is not None:
            writer = index.getWriter()
        corps = storage.loadCorpora(corpora_id)
        if corps is None:
            corps = Corpora( corpora_id )
        fileGenerator = reader.parseFile( corps )
        docindex=[]
        ngramindex=[]
        # fisrt part of file parsing
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                _logger.debug(document['id'])
                document.addEdge('Corpus', corpusNum, 1)
                if index is not None:
                    res = index.write(document, writer, overwrite)
                    if res is not None:
                        _logger.debug("document will not be overwritten")
                ngramindex+=self._extractNGrams( storage, document, corpusNum,\
                    ngramMin, ngramMax, filters, stopwords)
                # TODO export docngrams (filtered)
                # Document-corpus Association are included in the object
                docindex+=[document['id']]

        # Second part of file parsing
        except StopIteration, stop:
            if index is not None:
                # commit changes to indexer
                writer.commit()
            # insert or updates corpora
            corps = reader.corpora
            storage.insertCorpora( corps )
            # insert the new corpus
            for corpusNum in corps['content']:
                # get the Corpus object and import
                corpus = reader.corpusDict[ corpusNum ]
                for docid in docindex:
                    corpus.addEdge('Document', docid, 1)
                for ngid in ngramindex:
                    corpus.addEdge('NGram', ngid, 1)
                corpus.addEdge('Corpora', corpora_id, 1)
                storage.insertCorpus(corpus)
                storage.insertAssocCorpus(corpus['id'], corps['id'])
                yield corpus, corpora_id
                del reader.corpusDict[ corpusNum ]
            return

    def _extractNGrams(self, storage, document, corpusNum, ngramMin,\
        ngramMax, filters, stopwords):
        """"Main NLP operation for a document"""
        _logger.debug(tokenizer.TreeBankWordTokenizer.__name__+\
            " is working on document "+ document['id'])
        # get filtered ngrams
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            stopwords, ngramMin, ngramMax, filters )
        # insert doc in storage
        #print document['edges']
        storage.insertDocument( document )
        for ngid, ng in docngrams.iteritems():
            # save document occurrences and delete it
            docOccs = ng['occs']
            del ng['occs']
            ng.addEdge( 'Document', document['id'], docOccs )
            ng.addEdge( 'Corpus', corpusNum, 1 )
            storage.insertNGram( ng )
        # clean tokens before ending
        document['tokens'] = []
        # returns the ngram id index for the document
        return docngrams.keys()
