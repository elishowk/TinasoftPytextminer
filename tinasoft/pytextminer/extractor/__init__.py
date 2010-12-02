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
    def __init__( self, storage, config, corpora, filters=None, stemmer=None ):
        self.reader = None
        self.config = config
        self.filters = []
        if filters is not None:
            self.filters = filters
        self.stemmer = stemmer
        # params from the controler
        self.corpora = corpora
        self.storage = storage
        # instanciate the tagger, takes times on learning
        self.tagger = tagger.TreeBankPosTagger(
            training_corpus_size = self.config['training_tagger_size'],
            trained_pickle = self.config['tagger']
        )

    def _openFile(self, path, format ):
        """
        loads the source file
        automatically giving the Reader its config
        """
        dsn = format+"://"+path
        if format in self.config:
            return Reader( dsn, **self.config[format] )
        else:
            return Reader( dsn )


    def _walkFile( self, path, format ):
        """Main parsing generator method"""
        self.reader = self._openFile( path, format )
        fileGenerator = self.reader.parseFile()
        try:
            while 1:
                yield fileGenerator.next()
        except StopIteration:
            return

    def extract_file(self, path, format, extract_path, whitelistlabel, minoccs):
        """
        parses a source file,
        tokenizes and filters,
        stores Corpora and Corpus object
        then produces a whitelist,
        and finally export to a file
        """
        fileGenerator = self._walkFile(path, format)
        if whitelistlabel is None:
            whitelistlabel = self.corpora['id']
        newwl = whitelist.Whitelist(whitelistlabel, whitelistlabel)
        # basic counter
        doccount = 0
        try:
            while 1:
                # gets the next document
                document, corpusNum = fileGenerator.next()
                document.addEdge( 'Corpus', corpusNum, 1 )
                # extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(
                    document,
                    self.config,
                    self.filters,
                    self.tagger,
                    self.stemmer
                )
                ### updates newwl to prepare export
                if  corpusNum not in newwl['corpus']:
                    newwl['corpus'][corpusNum] = corpus.Corpus(corpusNum)
                newwl['corpus'][corpusNum].addEdge('Document', document['id'], 1)

                for ng in docngrams.itervalues():
                    newwl.addContent( ng, corpusNum, document['id'] )

                doccount += 1
                if doccount % NUM_DOC_NOTIFY == 0:
                    _logger.debug("%d documents parsed"%doccount)
                yield doccount

                newwl.storage.flushNGramQueue()

        except StopIteration:
            newwl.storage.flushNGramQueue()
            _logger.debug("Total documents extracted = %d"%doccount)
            self.storage.updateCorpora( self.corpora, False )
            whitelist_exporter = Writer("whitelist://"+extract_path)
            newwl = whitelist_exporter.write_whitelist(newwl, minoccs)
            return

    def index_file(self, path, format, whitelist, overwrite=False):
        """given a white list, indexes a source file to storage"""
        ### adds whitelist as a unique filter
        self.duplicate = []
        self.filters = [whitelist]
        fileGenerator = self._walkFile( path, format )
        doccount = 0
        try:
            while 1:
                ### gets the next document
                document, corpusNum = fileGenerator.next()
                document.addEdge( 'Corpus', corpusNum, 1 )
                ### updates Corpora and Corpus objects edges
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)
                ### adds Corpus-Doc edge if possible
                self.reader.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)
                ### extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(
                    document,
                    self.config,
                    self.filters,
                    self.tagger,
                    stemmer.Identity()
                )
                nlemmas = tokenizer.TreeBankWordTokenizer.group(docngrams, whitelist)
                #### inserts/updates NGram and update document obj
                self._insert_NGrams(nlemmas, document, corpusNum, overwrite)
                # creates or OVERWRITES document into storage
                self.duplicate += self.storage.updateDocument( document, overwrite )
                doccount += 1
                if doccount % NUM_DOC_NOTIFY == 0:
                    _logger.debug("%d documents indexed"%doccount)
                yield doccount

        except StopIteration:
            self.storage.updateCorpora( self.corpora, overwrite )
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            return

    def _insert_NGrams( self, docngrams, document, corpusNum, overwrite ):
        """
        Inserts NGrams and its graphs to storage
        MUST NOT BE USED FOR AN EXISTING DOCUMENT IN THE DB
        OTHERWISE THIS METHOD WILL BREAK EXISTING DATA
        """
        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            # updates NGram-Corpus edges
            storedDoc = self.storage.loadDocument( document['id'] )
            ### document is not in the database
            if storedDoc is None:
                ng.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            else:
                ### document exists but not attached to the current period
                if corpusNum not in storedDoc['edges']['Corpus']:
                    ng.addEdge( 'Corpus', corpusNum, 1 )
                    self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
                ### document exist but does not contains the current ngram
                if ngid not in storedDoc['edges']['NGram']:
                    ng.addEdge( 'Corpus', corpusNum, 1 )
                    self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            ### anyway attach document and ngram
            ng.addEdge( 'Document', document['id'], docOccs )
            # updates Doc-NGram edges
            document.addEdge( 'NGram', ng['id'], docOccs )
            # queue the storage/update of the ngram
            self.storage.updateNGram( ng, overwrite, document['id'], corpusNum )
        self.storage.flushNGramQueue()
