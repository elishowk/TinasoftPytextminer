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
        corpusField: 'corp_num',
        docField: 'doc_num',
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
        for doc in self:
            if doc is None: continue
            self.line += 1
            tmpfields = dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusID = doc[self.fields['corpusField']]
                del tmpfields['corpusField']
                #if not isinstance( corpusID, unicode ) or not isinstance( corpusID, str ):
                #    raise Exception("%s is not in string format"%corpusID)
            except Exception, exc:
                _logger.error( "tinacsv error : corpus id missing at line %d"%self.line )
                _logger.error( exc )
                continue
            # TODO check if corpus already exists
            newdoc = self._parse_document( doc, tmpfields, corpusID )
            if newdoc is None: continue
            # if corpus DOES NOT already exist
            if corpusID not in self.corpusDict:
                # creates a new corpus and adds it to the global dict
                self.corpusDict[ corpusID ] = corpus.Corpus( corpusID )
            # sends the document and the corpus id
            yield newdoc, corpusID

    def _parse_document( self, doc, tmpfields, corpusNum ):
        """
        parses a row to extract a document object
        with its edges
        """
        docArgs = {}
        # parsing TRY
        try:
            # get required fields
            docID = doc[tmpfields['docField']]
            content = doc[tmpfields['contentField']]
            title  = doc[tmpfields['titleField']]
            del tmpfields['docField']
            del tmpfields['contentField']
            del tmpfields['titleField']
            #if not isinstance( docID, unicode ) or not isinstance( docID, str ):
            #    raise Exception("%s is not in string format"%docID)
        except Exception, exc:
            _logger.error("error parsing document %s at line %d : skipping"%(docID,self.line))
            _logger.error(exc)
            return None
        # parsing optional fields loop and TRY
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = doc[ field ]
            except Exception, exc:
                _logger.warning("missing a document's field %s at line %d"%(field,self.line))

        if 'dateField' in docArgs:
            datestamp = docArgs[ 'dateField' ]
        else:
            datestamp = None
        # new document
        newdoc = document.Document(
            content,
            docID,
            title,
            datestamp=datestamp,
            **docArgs
        )
        return newdoc
