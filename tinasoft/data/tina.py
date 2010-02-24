# -*- coding: utf-8 -*-

from tinasoft.data import basecsv
from tinasoft.pytextminer import document, corpus, tokenizer
import codecs
import csv
import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer (basecsv.Importer):
    """
    tina csv format
    instanciation example:
    from tinasoft.data import Reader
    tinaReader = Reader( "tina://text_file_to_import.csv", fields={
        titleField: 'doc_titl',
        contentField: 'doc_abst',
        authorField: 'doc_acrnm',
        corpusNumberField: 'corp_num',
        docNumberField: 'doc_num',
        index1Field: 'index_1',
        index2Field: 'index_2',
        dateField: 'date',
        keywordsField: 'doc_keywords',
    })
    """
    #docDict = {}
    corpusDict = {}

    def parseFile( self, corpora ):
        """parses a row to extract corpus meta-data"""
        self.corpora = corpora
        for doc in self.csv:
            tmpfields=dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = self.decode( doc[self.fields['corpusNumberField']] )
                del tmpfields['corpusNumberField']
            except Exception, exc:
                _logger.error( "file parsing error : corpus number is required" )
                _logger.error( exc )
                continue
            # TODO check if corpus already exists
            newdoc = self.parseDocument( doc, tmpfields, corpusNumber )
            if newdoc is None:
                _logger.debug( "skipping a document" )
                continue
            # updates corpus-corpora edges : must occur only once
            if corpusNumber not in self.corpora['edges']['Corpus']:
                self.corpora.addEdge( 'Corpus', corpusNumber, 1 )
                self.corpora['content'] += [ corpusNumber ]
                self.corpusDict[ corpusNumber ].addEdge( 'Corpora', self.corpora['id'], 1)
            # if corpus NOT already exists
            if corpusNumber not in self.corpusDict:
                # creates a new corpus and adds it to the global dict
                newcorpus = corpus.Corpus( corpusNumber )
                # adds the corpus to internal attributes
                self.corpusDict[ corpusNumber ] = newcorpus
            self.corpusDict[ corpusNumber ]['content'] += [ newdoc['id'] ]
            self.corpusDict[ corpusNumber ].addEdge( 'Document', newdoc['id'], 1)

            # sends the document and the corpus id
            yield newdoc, corpusNumber

    def parseDocument( self, doc, tmpfields, corpusNum ):
        """parses a row to extract a document object"""
        docArgs = {}
        # parsing TRY
        try:
            # get required fields
            docNum = self.decode( doc[tmpfields[ 'docNumberField' ]] )
            content = self.decode( doc[tmpfields[ 'contentField' ]] )
            title  = self.decode( doc[tmpfields[ 'titleField' ]] )
            del tmpfields['docNumberField']
            del tmpfields['contentField']
            del tmpfields['titleField']
        except Exception, exc:
            _logger.debug("error parsing doc "+str(docNum)+" from corpus "+str(corpusNum))
            _logger.debug(exc)
            return None
        # parsing optional fields loop and TRY
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = self.decode( doc[ field ] )
            except Exception, exc:
                _logger.debug("unable to parse opt field "+field+" in document " + str(docNum))
                _logger.debug(exc)
        if 'dateField' in docArgs:
            datestamp = docArgs[ 'dateField' ]
        else:
            datestamp = None
        # new document
        newdoc = document.Document(
            content,
            docNum,
            title,
            datestamp=datestamp,
            **docArgs
        )
        # add a corpus edge
        newdoc.addEdge('Corpus', corpusNum, 1)
        return newdoc
