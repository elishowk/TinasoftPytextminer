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

    def _open_reader(self, path, format ):
        """
        loads the source file
        automatically giving the Reader its config
        """
        dsn = format+"://"+path
        if format in self.config:
            return Reader( dsn, **self.config[format] )
        else:
            return Reader( dsn )


    def _walk_file( self, path, format ):
        """Main parsing generator method"""
        self.reader = self._open_reader( path, format )
        fileGenerator = self.reader.parse_file()
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
        fileGenerator = self._walk_file(path, format)
        self.corpusDict = {}
        if whitelistlabel is None:
            whitelistlabel = self.corpora['id']
        newwl = whitelist.Whitelist(whitelistlabel, whitelistlabel)
        # basic counter
        doccount = 0
        try:
            while 1:
                # gets the next document
                document, corpusNum = fileGenerator.next()
                # if corpus DOES NOT already exist
                if corpusNum not in self.corpusDict:
                    # creates a new corpus and adds it to the global dict
                    self.corpusDict[ corpusNum ] = corpus.Corpus( corpusNum )
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
                    newwl.addEdge("NGram", ng['id'], 1)
                newwl.storage.flushNGramQueue()

                doccount += 1
                if doccount % NUM_DOC_NOTIFY == 0:
                    _logger.debug("%d documents parsed"%doccount)
                yield doccount

        except StopIteration:
            _logger.debug("Total documents extracted = %d"%doccount)
            self.storage.updateCorpora( self.corpora, False )
            whitelist_exporter = Writer("whitelist://"+extract_path)
            (filepath, newwl) = whitelist_exporter.write_whitelist(newwl, minoccs)
            del newwl
            return

    def index_file(self, path, format, whitelist, overwrite=False):
        """given a white list, indexes a source file to storage"""
        ### adds whitelist as a unique filter
        self.duplicate = []
        self.filters = [whitelist]
        fileGenerator = self._walk_file( path, format )
        self.corpusDict = {}
        doccount = 0
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                # if corpus DOES NOT already exist
                if corpusNum not in self.corpusDict:
                    # creates a new corpus and adds it to the global dict
                    self.corpusDict[ corpusNum ] = corpus.Corpus( corpusNum )
                document.addEdge( 'Corpus', corpusNum, 1 )
                ### updates Corpora and Corpus objects edges
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)
                ### adds Corpus-Doc edge if possible
                self.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)
                ### extracts and filters ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(
                    document,
                    self.config,
                    self.filters,
                    self.tagger,
                    stemmer.Identity()
                )
                nlemmas = tokenizer.TreeBankWordTokenizer.group(docngrams, whitelist)
                #### inserts/updates NGram and update document obj
                self._update_NGram_Document(nlemmas, document, corpusNum)
                doccount += 1
                if doccount % NUM_DOC_NOTIFY == 0:
                    _logger.debug("%d documents indexed"%doccount)
                yield document['id']

        except StopIteration:
            self.storage.updateCorpora( self.corpora )
            for corpusObj in self.corpusDict.values():
                #import pdb; pdb.set_trace()
                _logger.debug("%d NEW NGrams in corpus %s"%(len(corpusObj.edges['NGram'].keys()), corpusObj.id))
                _logger.debug("found %d Documents in Corpus %s"%(len(corpusObj.edges['Document'].keys()), corpusObj.id))
                self.storage.updateCorpus( corpusObj )
            return

    def _update_NGram_Document( self, docngrams, document, corpusNum ):
        """
        Inserts NGrams and its relations to storage
        Verifying previous storage contents preventing data corruption
        Updates Document
        """
        storedDoc = self.storage.loadDocument( document['id'] )
        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            ### document is not in the database
            if storedDoc is None:
                ng.addEdge( 'Corpus', corpusNum, 1 )
                self.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            else:
                ### document exists but not attached to the current period
                if corpusNum not in storedDoc['edges']['Corpus']:
                    ng.addEdge( 'Corpus', corpusNum, 1 )
                    self.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
                ### document exist but does not contains the current ngram
                if ngid not in storedDoc['edges']['NGram']:
                    ng.addEdge( 'Corpus', corpusNum, 1 )
                    self.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            ### updates Doc-NGram edges
            ng.addEdge( 'Document', document['id'], docOccs )
            document.addEdge( 'NGram', ng['id'], docOccs )
            # queue the storage/update of the ngram
            self.storage.updateManyNGram( ng )

        self.storage.flushNGramQueue()

        if storedDoc is not None:
            self.duplicate += [storedDoc]
        self.storage.updateDocument( document )
