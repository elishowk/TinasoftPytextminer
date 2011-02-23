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

__author__="elishowk@nonutc.fr"

from tinasoft.pytextminer import corpus, tagger, tokenizer, whitelist, stemmer
from tinasoft.data import Reader, Writer

import re

import logging
_logger = logging.getLogger('TinaAppLogger')

NUM_DOC_NOTIFY = 20

class Extractor():
    """
    A universal source file extractor/importer
    """
    def __init__( self,
        config,
        path,
        format,
        storage,
        corpora,
        filters,
        stemmer,
        tokenizer,
        whitelist=None ):
        """

        """
        self.config = config
        self.fileGenerator = self._walk_file( path, format )
        # params from the controler
        self.corpora = corpora
        self.storage = storage
        self.filters = filters
        self.stemmer = stemmer
        self.tokenizer = tokenizer
        self.whitelist = whitelist
        # instanciate the tagger, takes times on learning if not already pickled
        self.tagger = tagger.TreeBankPosTagger(
            training_corpus_size = self.config['training_tagger_size'],
            trained_pickle = self.config['tagger']
        )
        # caching objects
        self.corpusDict = {}
        self.duplicate = []
        
    def _open_reader(self, path, format ):
        """
        loads the source file
        automatically giving the Reader its config
        """
        dsn = format+"://"+path
        config = self.config[format]
        config['doc_extraction'] = self.config['doc_extraction']
        if format in self.config:
            return Reader( dsn, **self.config[format] )
        else:
            raise RuntimeError("unable to load %s because its file definition was NOT found in the config file"%path)
            return


    def _walk_file( self, path, format ):
        """Main parsing generator method"""
        reader = self._open_reader( path, format )
        fileGenerator = reader.parse_file()
        try:
            while 1:
                yield fileGenerator.next()
        except StopIteration:
            return

    def _linkStoreNGrams( self, docngrams, document, corpusId ):
        """
        Updates NGram's links
        Verify storage contents preventing data corruption
        """
        storedDoc = self.storage.loadDocument( document['id'] )

        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            ### overwrites Doc-NGram edges
            ng.addEdge( 'Document', document['id'], docOccs )
            document.addEdge( 'NGram', ng['id'], docOccs )
            ### document is not in the storage OR there's no storage opened
            if storedDoc is None:
                ng.addEdge( 'Corpus', corpusId, 1 )
                self.corpusDict[ corpusId ].addEdge( 'NGram', ngid, 1 )
            ### document is already stored, safely increment links
            else:
                # feeds back duplicates
                self.duplicate += [storedDoc]
                ### document exists but not attached to the current period
                if corpusId not in storedDoc['edges']['Corpus']:
                    ng.addEdge( 'Corpus', corpusId, 1 )
                    self.corpusDict[ corpusId ].addEdge( 'NGram', ngid, 1 )
                ### document exist but does not contains the current ngram
                if ngid not in storedDoc['edges']['NGram']:
                    ng.addEdge( 'Corpus', corpusId, 1 )
                    self.corpusDict[ corpusId ].addEdge( 'NGram', ngid, 1 )
            self.storage.updateManyNGram( ng )
        self.storage.flushNGramQueue()
        return

    def _linkDocuments(self, document, corpus):
        corpusId = corpus['id']
        # if corpus DOES NOT already exist
        if corpus['id'] not in self.corpusDict:
            # creates a new corpus and adds it to the global dict
            self.corpusDict[ corpusId ] = corpus

        document.addEdge( 'Corpus', corpusId, 1 )
        self.corpusDict[ corpusId ].addEdge( 'Document', document['id'], 1)

        ### updates Corpora and Corpus objects edges
        self.corpora.addEdge( 'Corpus', corpusId, 1 )
        self.corpusDict[ corpusId ].addEdge( 'Corpora', self.corpora['id'], 1)

    def index(self):
        """
        Main method indexing a data source to the storage
        parses a source file, then tokenizes, groups and filters NGrams
        finally stores the whole graph database
        """
        doccount = 0
        try:
            while 1:
                document, corpus = self.fileGenerator.next()
                self._linkDocuments(document, corpus)
                ### extracts and filters ngrams
                docngrams = self.tokenizer.extract(
                    document,
                    self.config,
                    self.filters,
                    self.tagger,
                    self.stemmer,
                    self.whitelist
                )
                self._linkStoreNGrams(docngrams, document, corpus['id'])
                self.storage.updateDocument( document )

                doccount += 1
                if doccount % NUM_DOC_NOTIFY == 0:
                    _logger.debug("%d documents indexed"%doccount)
                yield document['id']

        except StopIteration:
            self.storage.updateCorpora( self.corpora )
            for corpusObj in self.corpusDict.values():
                _logger.debug("%d NEW NGrams in corpus %s"%(len(corpusObj.edges['NGram'].keys()), corpusObj.id))
                _logger.debug("found %d Documents in Corpus %s"%(len(corpusObj.edges['Document'].keys()), corpusObj.id))
                self.storage.updateCorpus( corpusObj )
            return


#    def extract_file(self, path, format, extract_path, whitelistlabel, minoccs):
#        """
#
#        """
#        # basic counter
#        doccount = 0
#        try:
#            while 1:
#                # gets the next document
#                document, corpus = fileGenerator.next()
#                #
#                self._linkDocuments(document, corpus)
#                # extract and filter ngrams
#                docngrams = tokenizer.extract(
#                    document,
#                    self.config,
#                    self.filters,
#                    self.tagger,
#                    self.stemmer
#                )
#                ### updates NGram Links
#                docngrams = self._linkNGrams(docngrams, document, corpus['id'])
#                ### stores NGrams into the whitelist
#                for ng in docngrams.itervalues():
#                    newwl.addContent( ng )
#                    newwl.addEdge("NGram", ng['id'], 1)
#                newwl.storage.flushNGramQueue()
#
#                doccount += 1
#                if doccount % NUM_DOC_NOTIFY == 0:
#                    _logger.debug("%d documents parsed"%doccount)
#                yield doccount
#
#        except StopIteration:
#            _logger.debug("Total documents extracted = %d"%doccount)
#            newwl.corpus = self.corpusDict
#            whitelist_exporter = Writer("whitelist://"+extract_path)
#            whitelist_exporter.write_whitelist(newwl, minoccs)
#            return
            