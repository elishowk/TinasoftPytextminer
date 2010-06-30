#!/usr/bin/python
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

from tinasoft.pytextminer import corpus, tagger, stopwords, tokenizer, filtering, whitelist
from tinasoft.data import Engine, Reader, Writer

import traceback
import time
#from mapreduce import ExtractionJob

import logging
_logger = logging.getLogger('TinaAppLogger')

class Extractor():
    """A source file importer = data set = corpora"""
    def __init__( self, storage, config, corpora, stopwds, index=None ):
        self.reader = None
        self.config=config
        # load Stopwords object
        self.stopwords = stopwds
        #filtertag = filtering.PosTagFilter()
        filterContent = filtering.Content()
        validTag = filtering.PosTagValid()
        self.filters = [filterContent,validTag]
        # keep duplicate document objects
        self.duplicate = []
        # params from the controler
        self.corpora = corpora
        self.storage = storage
        # indexer is NOT YET in USE
        self.index = None
        if self.index is not None:
            self.writer = index.getWriter()
        # instanciate the tagger, takes times on learning
        self.tagger = tagger.TreeBankPosTagger(training_corpus_size=self.config['training_tagger_size'])
        #self.__insert_threads = []

    def _indexDocument( self, documentobj, overwrite ):
        """eventually index the document's text"""
        if self.index is not None:
            if self.index.write(documentobj, self.writer, overwrite) is None:
                _logger.error("document content indexation failed :" \
                    + str(documentobj['id']))

    def _openFile(self, path, format ):
        """loads the source file"""
        dsn = format+"://"+path
        if format in self.config:
            return Reader( dsn, **self.config[format] )
        else:
            return Reader( dsn )


    def _walkFile( self, path, format ):
        """Main parsing method"""
        self.reader = self._openFile( path, format )
        fileGenerator = self.reader.parseFile()
        try:
            while 1:
                yield fileGenerator.next()
        except StopIteration:
            return

    def extract_file(self, path, format, extract_path, minoccs=1):
        # starts the parsing
        fileGenerator = self._walkFile( path, format )
        newwl = whitelist.Whitelist(self.corpora['id'], self.corpora['id'], corpus={})
        doccount = 0
        try:
            while 1:
                # gets the next document
                document, corpusNum = fileGenerator.next()
                corpusNum = str(corpusNum)
                # extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(\
                    document,\
                    self.stopwords,\
                    self.config['ngramMin'],\
                    self.config['ngramMax'],\
                    self.filters, \
                    self.tagger\
                )
                # increments number of docs per period
                if  corpusNum not in newwl['corpus']:
                    #_logger.debug( "adding a period to the whitelist : " + corpusNum )
                    newwl['corpus'][corpusNum] = corpus.Corpus(corpusNum)
                newwl['corpus'][corpusNum].addEdge('Document', str(document['id']), 1)
                for ngid, ng in docngrams.iteritems():
                    if ngid not in newwl['content']:
                        newwl['content'][ngid] = ng
                        newwl['content'][ngid]['status'] = ""
                    # increments per period occs
                    newwl['content'][ngid].addEdge( 'Corpus', corpusNum, 1 )
                    # increments total occurences within the dataset
                    newwl.addEdge( 'NGram', ngid, 1 )
                doccount += 1
                if doccount % 10 == 0:
                    _logger.debug("%d documents parsed"%doccount)

        except StopIteration:
            _logger.debug("Total documents extracted = %d"%doccount)
            csvfile = Writer("whitelist://"+extract_path)
            (outpath,newwl) = csvfile.write_whitelist(newwl, minoccs)
            return outpath
        except Exception:
            _logger.error(traceback.format_exc())
            return False

    def extract_file_improved(self, path, format, extract_path, minoccs=1, overwrite=False):
        """intented to extract a dataset to a whitelist file, only storing corpora, corpus, document object into storage"""
        # starts the parsing
        fileGenerator = self._walkFile( path, format )
        newwl = whitelist.Whitelist(self.corpora['id'], self.corpora['id'], corpus={})
        doccount = 0
        try:
            while 1:
                ### gets the next document
                document, corpusNum = fileGenerator.next()
                ### updates Corpora and Corpus objects edges
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)
                # adds Corpus-Doc edge if possible
                self.reader.corpusDict[ corpusNum ].addEdge( 'Document', document['id'], 1)
                ### extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract(\
                    document,\
                    self.stopwords,\
                    self.config['ngramMin'],\
                    self.config['ngramMax'],\
                    self.filters, \
                    self.tagger\
                )
                ### updates newwl object : increments number of docs per period
                if  corpusNum not in newwl['corpus']:
                    newwl['corpus'][corpusNum] = corpus.Corpus(corpusNum)
                newwl['corpus'][corpusNum].addEdge('Document', str(document['id']), 1)
                for ngid, ng in docngrams.iteritems():
                    if ngid not in newwl['content']:
                        newwl['content'][ngid] = ng
                        newwl['content'][ngid]['status'] = ""
                    # increments per period occs
                    newwl['content'][ngid].addEdge( 'Corpus', corpusNum, 1 )
                    # increments total occurences within the dataset
                    newwl.addEdge( 'NGram', ngid, 1 )
                #### inserts/updates document, corpus and corpora
                # TODO test removing self.reader.corpusDict from memory, use the DB !!
                self.storage.updateCorpora( self.corpora, overwrite )
                for corpusObj in self.reader.corpusDict.values():
                    self.storage.updateCorpus( corpusObj, overwrite )
                self.reader.corpusDict = {}
                self._update_Document(overwrite, corpusNum, document)
                doccount += 1
                if doccount % 10 == 0:
                    _logger.debug("%d documents parsed"%doccount)
                # doc's indexation
                self._indexDocument( document, overwrite )
        except StopIteration:
            _logger.debug("Total documents extracted = %d"%doccount)
            csvfile = Writer("whitelist://"+extract_path)
            (outpath,newwl) = csvfile.write_whitelist(newwl, minoccs)
            _logger.debug(self.duplicate)
            return outpath
        except Exception:
            _logger.error(traceback.format_exc())
            return False

    def _update_Document(self, overwrite, corpusNum, document):
        """ doc's storage : checks if exists, then updates the necessary"""
        if overwrite is False:
            storedDoc = self.storage.loadDocument( document['id'] )
            if storedDoc is not None:
                # force add the doc-corpus edge : aims at at attaching a doc to multiple corpus
                storedDoc.addEdge( 'Corpus', corpusNum, 1 )
                # force update
                self.storage.updateDocument( storedDoc, True )
                _logger.warning( "Doc %s is already stored : only updating edges"%document['id'] )
                # skip document
                self.duplicate += [document]
                return False
        # anyway, insert document to storage
        self.storage.updateDocument( document, True )
        return True

    def import_file(self, path, format, overwrite=False):

        # opens and starts walking a file
        fileGenerator = self._walkFile( path, format )
        # 1st part = ngram extraction
        doccount = 0
        try:
            while 1:
                # document parsing, doc-corpus edge is written
                document, corpusNum = fileGenerator.next()

                # add Corpora-Corpus edges if possible
                self.corpora.addEdge( 'Corpus', corpusNum, 1 )
                self.reader.corpusDict[ corpusNum ].addEdge( 'Corpora', self.corpora['id'], 1)

                # doc's indexation
                self._indexDocument( document, overwrite )

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
                self.reader.corpusDict = {}
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
                self._insert_NGrams( \
                    document, \
                    corpusNum,\
                    self.config['ngramMin'], \
                    self.config['ngramMax'], \
                    overwrite \
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

    def _insert_NGrams( self, document, corpusNum, ngramMin, ngramMax, overwrite ):
        """
        MUST NOT BE USED FOR AN EXISTING DOCUMENT IN THE DB
        Main NLP operations and extractions on a document
        FOR DOCUMENT THAT IS NOT ALREADY IN THE DATABASE
        """
        # extract filtered ngrams
        docngrams = tokenizer.TreeBankWordTokenizer.extract( \
            document,\
            self.stopwords, \
            ngramMin, \
            ngramMax, \
            self.filters, \
            self.tagger \
        )
        # insert into database
        #before=time.time()
        for ngid, ng in docngrams.iteritems():
            # increments document-ngram edge
            docOccs = ng['occs']
            del ng['occs']
            # init ngram's edges
            ng.addEdge( 'Corpus', corpusNum, 1 )
            ng.addEdge( 'Document', document['id'], docOccs )
            # updates doc-ngram and corpus-ngram edges
            document.addEdge( 'NGram', ng['id'], docOccs )
            self.reader.corpusDict[ corpusNum ].addEdge( 'NGram', ngid, 1 )
            # queue the update of the ngram
            self.storage.updateNGram( ng, overwrite, document['id'], corpusNum )
        # creates or OVERWRITES document into storage
        self.storage.insertDocument( document, overwrite=True )
        self.storage.flushNGramQueue()
        self.storage.commitAll()
        #inserttime = time.time() - before
        #self.totaltime += inserttime
        #meantime = self.totaltime / self.doccount
        #_logger.debug( "insert and commit ngrams and document = %s sec, mean = %s sec"%(str(inserttime),str(meantime)) )


