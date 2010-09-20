# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk
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

__author__="Elias Showk"
from tinasoft.pytextminer import PyTextMiner

from tinasoft.pytextminer import corpus, tagger, stopwords, tokenizer, filtering, whitelist
from tinasoft.data import Engine, Reader, Writer

import re

import traceback

import logging
_logger = logging.getLogger('TinaAppLogger')

class Extractor():
    """A source file importer = data set = corpora"""
    def __init__( self, storage, config, corpora, stopwds, filters=None, stemmer=None ):
        self.reader = None
        self.config=config
        # load Stopwords object
        self.stopwords = stopwds
        #filtertag = filtering.PosTagFilter()
        filterContent = filtering.Content()
        validTag = filtering.PosTagValid(
            config= {
                'rules': re.compile(self.config['postag_valid'])
            }
        )
        self.filters = [filterContent,validTag]
        if filters is not None:
            self.filters += filters
        self.stemmer = stemmer
        # keep duplicate document objects
        self.duplicate = []
        # params from the controler
        self.corpora = corpora
        self.storage = storage
        # instanciate the tagger, takes times on learning
        self.tagger = tagger.TreeBankPosTagger(
            training_corpus_size=self.config['training_tagger_size'],
            trained_pickle=self.config['tagger']
        )
        #self.__insert_threads = []

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

    def extract_file(self, path, format, extract_path, whitelistlabel=None, minoccs=1):
        """
        parses a source file,
        tokenizes and filters,
        stores Corpora and Corpus object
        then produces a whitelist,
        and finally export to a file
        """
        fileGenerator = self._walkFile( path, format )
        if whitelistlabel is None:
            whitelistlabel = self.corpora['id']
        newwl = whitelist.Whitelist(whitelistlabel, whitelistlabel)
        doccount = 0
        try:
            while 1:
                # gets the next document
                document, corpusNum = fileGenerator.next()
                document.addEdge( 'Corpus', corpusNum, 1 )
                # extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(
                    document,
                    self.stopwords,
                    self.config['ngramMin'],
                    self.config['ngramMax'],
                    self.filters,
                    self.tagger,
                    self.stemmer
                )
                ### updates newwl to prepare export
                # increments number of docs per period
                if  corpusNum not in newwl['corpus']:
                    newwl['corpus'][corpusNum] = corpus.Corpus(corpusNum)
                newwl['corpus'][corpusNum].addEdge('Document', str(document['id']), 1)
                for ngid, ng in docngrams.iteritems():
                    if ngid not in newwl['content']:
                        newwl['content'][ngid] = ng
                        newwl['content'][ngid]['status'] = ""
                    else:
                        newwl['content'][ngid] = PyTextMiner.updateEdges( ng, newwl['content'][ngid], ['Corpus','Document','label','postag'] )
                    # increments per period occs
                    newwl['content'][ngid].addEdge( 'Corpus', corpusNum, 1 )
                    # increments total occurences within the dataset
                    newwl.addEdge( 'NGram', ngid, 1 )
                doccount += 1
                if doccount % 100 == 0:
                    _logger.debug("%d documents parsed"%doccount)

        except StopIteration:
            _logger.debug("Total documents extracted = %d"%doccount)
            self.storage.updateCorpora( self.corpora, False )
            #for corpusObj in newwl['corpus'].values():
            #    self.storage.updateCorpus( corpusObj, False )
            csvfile = Writer("whitelist://"+extract_path)
            (outpath,newwl) = csvfile.write_whitelist(newwl, minoccs)
            return outpath
        except Exception:
            _logger.error(traceback.format_exc())
            return False

    def index_file(self, path, format, whitelist, overwrite=False):
        """given a white list, indexes a source file to storage"""
        ### adds whitelist as an additional filter
        self.filters += [whitelist]
        ### starts the parsing
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
                #document.addEdge('Corpus', corpusNum, 1)
                ### extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(
                    document,
                    self.stopwords,
                    self.config['ngramMin'],
                    self.config['ngramMax'],
                    self.filters,
                    self.tagger,
                    self.stemmer
                )
                #### inserts/updates document, corpus and corpora
                self._insert_NGrams(docngrams, document, corpusNum, overwrite)
                # creates or OVERWRITES document into storage
                del document['content']
                self.duplicate += self.storage.updateDocument( document, overwrite )
                doccount += 1
                if doccount % 10 == 0:
                    _logger.debug("%d documents indexed"%doccount)

        except StopIteration:
            self.storage.updateCorpora( self.corpora, overwrite )
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            return True
        except Exception:
            _logger.error(traceback.format_exc())
            return False

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
            if storedDoc is None:
                ng.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            elif corpusNum not in storedDoc['edges']['Corpus']:
                ng.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            elif ngid not in storedDoc['edges']['NGram']:
                ng.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            ng.addEdge( 'Document', document['id'], docOccs )
            # updates Doc-NGram edges
            document.addEdge( 'NGram', ng['id'], docOccs )
            # queue the update of the ngram
            self.storage.updateNGram( ng, overwrite, document['id'], corpusNum )
        self.storage.flushNGramQueue()

    def import_file(self, path, format, overwrite=False):
        """
        OBSOLETE
        """
        # opens and starts walking a file
        fileGenerator = self._walkFile( path, format )
        # 1st part = ngram extraction
        doccount = 0
        try:
            while 1:
                # document parsing, doc-corpus edge is written
                document, corpusNum = fileGenerator.next()
                document.addEdge( 'Corpus', corpusNum, 1 )

                # add Corpora-Corpus edges if possible
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)

                # adds Corpus-Doc edge if possible
                self.reader.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)

                doccount += 1
                if doccount % 10 == 0:
                    _logger.debug("%d documents parsed"%doccount)

                # inserts/updates corpus and corpora
                # TODO test removing self.reader.corpusDict from memory, use the DB !!
                self.storage.updateCorpora( self.corpora, overwrite )
                for corpusObj in self.reader.corpusDict.values():
                    self.storage.updateCorpus( corpusObj, overwrite )
                if self._update_Document(overwrite, corpusNum, document) is False:
                    continue
                # HUM, VERY UNSAFE
                #insertargs = (
                #    document,
                #    corpusNum,
                #    self.config['ngramMin'],
                #    self.config['ngramMax'],
                #    overwrite
                #)
                #t = Thread(target=self._insert_NGrams, args=insertargs)
                #t.start()
                #self.__insert_threads += [t]

                # document's ngrams extraction
                docngrams = tokenizer.TreeBankWordTokenizer.extract( \
                    document,\
                    self.stopwords, \
                    ngramMin, \
                    ngramMax, \
                    self.filters, \
                    self.tagger,
                    self.stemmer
                )
                self._insert_NGrams(
                    docngrams,
                    document,
                    corpusNum,
                    overwrite
                )
        # Second part of file parsing = document graph updating
        except StopIteration:
            # commit changes to indexer
            if self.index is not None:
                self.writer.commit()
            return True
        except Exception:
            _logger.error( traceback.format_exc() )
            return False

    def _update_Document(self, overwrite, corpusNum, document):
        """doc's storage : updates and returns duplicates"""
        self.duplicate += self.storage.updateDocument( document, overwrite )
        return True
