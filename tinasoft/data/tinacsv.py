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
        doc_title: 'doc_titl',
        doc_content: 'doc_abst',
        corpus_id: 'corp_id',
        doc_id 'doc_id',
        doc_author: 'doc_acrnm',
    })
    """
    def parseFile(self):
        """
        parses a row to extract corpus meta-data
        updates corpus and corpora edges
        """
        for doc in self:
            if doc is None: continue
            tmpfields = dict(self.fields)
            # decoding & parsing TRY
            try:
                corpusID = doc[self.fields['corpus_id']]
                del tmpfields['corpus_id']
            except Exception, exc:
                _logger.error( "tinacsv error : corpus id missing at line %d"%self.reader.line_num )
                _logger.error( exc )
                continue
            # TODO check if corpus already exists
            newdoc = self._parse_document( doc, tmpfields )
            # if error parsing the document
            if newdoc is None: continue
            # if corpus DOES NOT already exist
            if corpusID not in self.corpusDict:
                # creates a new corpus and adds it to the global dict
                self.corpusDict[ corpusID ] = corpus.Corpus( corpusID )
            # sends the document and the corpus id
            yield newdoc, corpusID

    def _parse_document( self, doc, tmpfields ):
        """
        parses a row to extract a document object
        with its edges
        """
        try:
            # get required fields
            docID = self._coerce_unicode( doc[tmpfields['doc_id']] )
            content = self._coerce_unicode( doc[tmpfields['doc_content']] )
            #title  = doc[tmpfields['doc_title']]
            label = self._coerce_unicode( doc[tmpfields[self.doc_label]] )
            del tmpfields['doc_id']
            del tmpfields['doc_content']
            del tmpfields[self.doc_label]
        except Exception, exc:
            _logger.error("error parsing document at line %d : skipping"%(self.reader.line_num))
            _logger.error(exc)
            return None
        # parsing optional fields loop and TRY
        docArgs = {}
        for field in tmpfields.itervalues():
            try:
                docArgs[ field ] = doc[ field ]
            except Exception, exc:
                _logger.warning("missing a document's field %s at line %d"%(field,self.reader.line_num))

        # new document
        newdoc = document.Document(
            content,
            docID,
            label,
            **docArgs
        )
        return newdoc
