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


from tinasoft.data import Importer as BaseImporter
from tinasoft.pytextminer import document
from tinasoft.pytextminer import corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

class Importer(BaseImporter):
    """
    importer of any tinasoft source file format
    configuration kwargs passed to construction must contain a 'fields' dict
    """

    def __init__(self, path, **kwargs):
        BaseImporter.__init__(self, path, **kwargs)

    def parse_file(self):
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
            except Exception, exc:
                _logger.error( "sourcefile error : corpus id missing at line %d"%self.line_num )
                _logger.error( exc )
                continue
            newdoc = self._parse_document( doc, tmpfields )
            # if error parsing the document
            if newdoc is None: continue
            # sends the document and the corpus id
            yield newdoc, corpus.Corpus(corpusID)
        self.file.close()
        return

    def _parse_document(self, doc, tmpfields):
        """
        parses a row to extract a document object
        with its edges
        """
        try:
            if tmpfields[self.doc_label] in doc:
                label = doc[ tmpfields[self.doc_label] ]
            else:
                _logger.warning("custom label %s NOT found, using default 'label' field (%s)"%(self.doc_label, tmpfields['label']))
                label = doc[tmpfields['label']]
                del tmpfields['label']
        except KeyError, exc:
            _logger.warning("unable to find any label, skipping document at line %d : %s"%(self.line_num, exc))
            return None
        try:
            # get required fields
            docID = doc[tmpfields['id']]
            content = doc[tmpfields['content']]
            # cleans all remaining fields
            if 'corpus_id' in tmpfields: del tmpfields['corpus_id']
            if 'content' in tmpfields: del tmpfields['content']
            if 'id' in tmpfields: del tmpfields['id']
        except Exception, exc:
            _logger.error("error parsing document at line %d : skipping"%(self.line_num))
            _logger.error(exc)
            return None
        # parsing optional fields loop and TRY
        docArgs = {}
        for field, realname in tmpfields.iteritems():
            try:
                docArgs[ field ] = doc[ realname ]
            except Exception, exc:
                _logger.warning("missing field %s at line %d"%(field,self.reader.line_num))
        for invalid_docarg in ["label","id","content","edges"]:
            if invalid_docarg in docArgs:
                del docArgs[invalid_docarg]
        docArgs['target'] = self.doc_extraction
        # new document
        newdoc = document.Document(
            content,
            docID,
            label,
            **docArgs
        )
        return newdoc
