# -*- coding: utf-8 -*-

from tinasoft import TinaApp
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
    corpusDict = {}

    def parseFile( self ):
        """
        parses a row to extract corpus meta-data
        updates corpus and corpora edges
        """
        self.line = 0
        for doc in self.csv:
            self.line += 1
            if self.line % 2 == 0:
                TinaApp.notify( None, 'tinasoft_runImportFile_running_status', str(self.line) )
            tmpfields=dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = self.decode( doc[self.fields['corpusNumberField']] )
                del tmpfields['corpusNumberField']
            except Exception, exc:
                _logger.error( "parsing error : corpus number is required at line %d"%self.line )
                _logger.error( exc )
                continue
            # TODO check if corpus already exists
            newdoc = self.parseDocument( doc, tmpfields, corpusNumber )
            if newdoc is None:
                _logger.error( "skipping a document" )
                continue
            # if corpus DOES NOT already exist
            if corpusNumber not in self.corpusDict:
                # creates a new corpus and adds it to the global dict
                self.corpusDict[ corpusNumber ] = corpus.Corpus( corpusNumber )
            # sends the document and the corpus id
            yield newdoc, corpusNumber

    def parseDocument( self, doc, tmpfields, corpusNum ):
        """
        parses a row to extract a document object
        with its edges
        """
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
            _logger.warning("parsing error "+str(docNum)+" at line "+str(self.line))
            _logger.warning(exc)
            return None
        # parsing optional fields loop and TRY
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = self.decode( doc[ field ] )
            except Exception, exc:
                _logger.warning("field %s was not found at line %s"%(field,str(self.line)))
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
