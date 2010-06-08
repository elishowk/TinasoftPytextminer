# -*- coding: utf-8 -*-
__author__="Elias Showk"
from tinasoft.pytextminer import corpus, tagger, stopwords, tokenizer, filtering, whitelist
from tinasoft.data import Engine, Reader, Writer

import traceback


import logging
_logger = logging.getLogger('TinaAppLogger')



class Extractor():
    """A source file importer = data set = corpora"""
    def __init__( self, storage, config, corpora, index=None ):
        self.reader = None
        self.config=config
        # load Stopwords object
        self.stopwords = stopwords.StopWords( "file://%s"%self.config['stopwords'] )
        #filtertag = filtering.PosTagFilter()
        filterContent = filtering.Content()
        validTag = filtering.PosTagValid()
        self.filters = [filterContent,validTag]
        # keep duplicate document objects
        self.duplicate = []
        # params from the controler
        self.corpora = corpora
        self.storage = storage
        self.index = index
        if self.index is not None:
            self.writer = index.getWriter()

        # instanciate the tagger, takes times on learning
        self.tagger = tagger.TreeBankPosTagger(training_corpus_size=self.config['training_tagger_size'])

    def _openFile(self, path, format ):
        """loads the source file"""
        dsn = format+"://"+path
        if format in self.config:
            return Reader( dsn, **self.config[format] )
        else:
            return Reader( dsn )

    def _indexDocument( self, documentobj, overwrite ):
        """eventually index the document's text"""
        if self.index is not None:
            if self.index.write(documentobj, self.writer, overwrite) is None:
                _logger.error("document content indexation failed :" \
                    + str(documentobj['id']))


    def _walkFile( self, path, format ):
        """Main parsing method"""
        self.reader = self._openFile( path, format )
        fileGenerator = self.reader.parseFile()
        count=0
        try:
            while 1:
                yield fileGenerator.next()
                count += 1
        except StopIteration:
            _logger.debug("Finished reading %d documents"%count)
            return

    def extract_file(self, path, format, extract_path, minoccs=1):
        # TODO : replace Counter by Whitelist object
        # starts the parsing
        fileGenerator = self._walkFile( path, format )
        newwl = whitelist.Whitelist(self.corpora['id'], None, self.corpora['id'])
        ngrams = {}
        periods = {}
        doccount = 0
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                # extract and filter ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract( \
                    document,\
                    self.stopwords, \
                    self.config['ngramMin'], \
                    self.config['ngramMax'], \
                    self.filters, \
                    self.tagger \
                )
                # increments number of docs per period
                if  corpusNum not in periods:
                    periods[corpusNum] = corpus.Corpus(corpusNum)
                periods[corpusNum].addEdge('Document',document['id'], 1)
                # increments per period total occurrences
                # newwl.addEdge( 'Corpus', corpusNum, 1 )
                for ngid, ng in docngrams.iteritems():
                    ng['status'] = ""
                    # increments total occurences within the dataset
                    newwl.addEdge( 'NGram', ngid, 1 )
                    # increments per corpus total occs
                    ng.addEdge( 'Corpus', corpusNum, 1 )
                    #newwl.addEdge( 'Normalized', ngid, newwl['edges']['NGram'][ngid]**len(ng['content']) )
                    #if ng['id'] not in ngrams:
                    #    ngrams[ng['id']] = ng
                    #else:
                    #    ngrams[ng['id']]['status'] = ng['status']
                    self.storage.insertNGram(ng)
                doccount += 1
                if doccount % 10000 == 0:
                    _logger.debug("%d documents parsed"%doccount)
        except StopIteration:
            _logger.debug("Total documents extracted = %d"%doccount)
            csvfile = Writer("whitelist://"+extract_path)
            for corpobj in periods.itervalues():
                _logger.debug( "period %s has got %d documents"%(corpobj['id'], len(corpobj['edges']['Document'].keys())) )
            return csvfile.write_whitelist(ngrams, newwl, periods, minoccs)
        except Exception:
            _logger.error(traceback.format_exc())
            return False

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
                # checks document in storage
                if overwrite is False:
                    storedDoc = self.storage.loadDocument( document['id'] )
                    if storedDoc is not None:
                        # add the doc-corpus edge if possible
                        storedDoc.addEdge( 'Corpus', corpusNum, 1 )
                        # force update
                        self.storage.updateDocument( storedDoc, True )
                        _logger.warning( "Doc %s is already stored : only updating edges"%document['id'] )
                        # skip document
                        self.duplicate += [document]
                        continue
                # document's ngrams extraction
                self.insertNGrams( \
                    document, \
                    corpusNum,\
                    self.config['ngramMin'], \
                    self.config['ngramMax'], \
                    overwrite \
                )
                doccount += 1
                if doccount % 10000 == 0:
                    _logger.debug("%d documents parsed"%doccount)
                # inserts/updates corpus and corpora
                self.storage.updateCorpora( self.corpora, overwrite )
                for corpusObj in self.reader.corpusDict.values():
                    self.storage.updateCorpus( corpusObj, overwrite )
        # Second part of file parsing = document graph updating
        except StopIteration:
            # commit changes to indexer
            if self.index is not None:
                self.writer.commit()
            # WARNING bottle-neck here on big big corpus
            # inserts/updates corpus and corpora
            #self.storage.updateCorpora( self.corpora, overwrite )
            #for corpusObj in self.reader.corpusDict.values():
             #   self.storage.updateCorpus( corpusObj, overwrite )
            #self.storage.flushNGramQueue()
            #self.storage.ngramindex = []
            self.storage.commitAll()
            return True
        except Exception:
            _logger.error( traceback.format_exc() )
            return False

    def insertNGrams( self, document, corpusNum, ngramMin, ngramMax, overwrite ):
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


