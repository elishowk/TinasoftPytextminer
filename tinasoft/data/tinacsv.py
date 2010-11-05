# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.


from tinasoft.data import basecsv
from tinasoft.pytextminer import document
from tinasoft.pytextminer import corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer (basecsv.Importer):
    """
    importer for tina csv format
    e.g. :
    from tinasoft.data import Reader
    tinaReader = Reader( "tinacsv://text_file_to_import.csv", config )
    config must contain a 'fields' dict, like :
    fields = {
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

    def parseFile(self):
        """
        parses a row to extract corpus meta-data
        updates corpus and corpora edges
        """
        self.line = 0
        #try:
        for doc in self:
            #doc = self.csv.next()
            if doc is None: continue
            self.line += 1
            tmpfields = dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusNumber = doc[self.fields['corpusNumberField']]
                del tmpfields['corpusNumberField']
            except Exception, exc:
                _logger.error( "tinacsv error : corpus id missing at line %d"%self.line )
                _logger.error( exc )
                continue
            # TODO check if corpus already exists
            newdoc = self._parse_document( doc, tmpfields, corpusNumber )
            if newdoc is None: continue
            # if corpus DOES NOT already exist
            if corpusNumber not in self.corpusDict:
                # creates a new corpus and adds it to the global dict
                self.corpusDict[ corpusNumber ] = corpus.Corpus( corpusNumber )
            # sends the document and the corpus id
            yield newdoc, corpusNumber
        #except StopIteration, sit:
        #    return

    def _parse_document( self, doc, tmpfields, corpusNum ):
        """
        parses a row to extract a document object
        with its edges
        """
        docArgs = {}
        # parsing TRY
        try:
            # get required fields
            docNum = doc[tmpfields['docNumberField']]
            content = doc[tmpfields['contentField']]
            title  = doc[tmpfields['titleField']]
            del tmpfields['docNumberField']
            del tmpfields['contentField']
            del tmpfields['titleField']
        except Exception, exc:
            _logger.error("error parsing document %s at line %d : skipping"%(str(docNum),self.line))
            _logger.error(exc)
            return None
        # parsing optional fields loop and TRY
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = doc[ field ]
            except Exception, exc:
                _logger.warning("missing a document's field %s at line %s"%(field,str(self.line)))
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
        return newdoc
