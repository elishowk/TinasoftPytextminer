# -*- coding: utf-8 -*-

from tinasoft.data import tinabsddb, basecsv
from tinasoft.pytextminer import document, corpus, ngram
import codecs
import csv
import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer (basecsv.Importer):

    docDict = {}
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
                print "document parsing exception (no corpus number avalaible) : ", exc
            # TODO check if corpus already exists
            document = self.parseDocument( doc, tmpfields, corpusNumber )
            if document is None:
                _logger.debug( "skipping a document" )
            else:
                # sends the document and the corpus id
                yield document, corpusNumber
            if self.corpusDict.has_key(corpusNumber) and \
                corpusNumber in self.corpora['content']:
                self.corpusDict[ corpusNumber ]['content'].append( document.id )
            corp = corpus.Corpus(
                corpusNumber,
                period_start = None,
                period_end = None,
            )
            corp['content'].append( document.id )
            # adds the corpus to internal attributes
            self.corpusDict[ corpusNumber ] = corp
            self.corpora['content'][ corpusNumber ] = 1

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

        doc = document.Document(
            content,
            docNum,
            title,
            datestamp=datestamp,
            edges={'Corpus':{ corpusNum: 1 }},
            **docArgs
        )
        return doc
