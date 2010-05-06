# -*- coding: utf-8 -*-
__author__="Elias Showk"
from tinasoft.pytextminer import tagger, stopwords, tokenizer, ngram
from tinasoft.data import Engine, Reader, Writer

# configuration file parsing
#import yaml

import logging
_logger = logging.getLogger('TinaAppLogger')


class Counter():
    """Utility object counting ngrams for different period"""
    def __init__(self):
        self.index={}

    def add(self, docngrams, corpus):
        if corpus not in self.index:
            self.index[corpus]={}
        for ngid, ng in docngrams.iteritems():
            if ngid in self.index[corpus]:
                self.index[corpus][ngid]['occs'] += 1
            else:
                self.index[corpus][ngid] = {}
                self.index[corpus][ngid]['label'] = ng['label']
                self.index[corpus][ngid]['postag'] = tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
                self.index[corpus][ngid]['occs'] = 1

class Extractor():
    """A source file importer = data set = corpora"""
    def __init__( self, storage, config, corpora, index=None ):

        self.config=config

        # init by methods
        self.filters=None
        self.stopwords=None

        # params from the controler
        self.corpora = corpora
        self.storage = storage
        self.index = index
        if self.index is not None:
            self.writer = index.getWriter()

        # instanciate the tagger, takes times on learning
        self.tagger = tagger.TreeBankPosTagger()

    def _openFile(self,
            path,
            format='tina'
        ):
        #try:
            # import import config yaml
        #    self.config = yaml.safe_load( file( configFile, 'rU' ) )
        #except yaml.YAMLError, exc:
        #    _logger.error( "Unable to read the importFile : "+exc )
        #    return False
        # load Stopwords object
        self.stopwords = stopwords.StopWords( "file://%s"%self.config['stopwords'] )

        #filtertag = ngram.PosTagFilter()
        filterContent = ngram.Filter()
        validTag = ngram.PosTagValid()
        self.filters = [filterContent,validTag]

        # loads the source file
        dsn = format+"://"+path
        fileReader = Reader( dsn, **self.config[format] )
        #    delimiter = self.config['delimiter'],
        #    quotechar = self.config['quotechar'],
        #    locale = self.config['locale'],
        #    fields = self.config['fields']
        #)
        return fileReader

    def _indexDocument( self, documentobj, overwrite ):
        if self.index is not None:
            if self.index.write(document, self.writer, overwrite) is None:
                _logger.debug("document content indexation skipped :" \
                    + str(document['id']))


    def _walkFile( self, path, format ):
        """Main parsing source file method"""
        # starts parsing
        self.reader = self._openFile( path, format )
        fileGenerator = self.reader.parseFile()
        count=0
        try:
            while 1:
                document, corpus = fileGenerator.next()
                _logger.debug( "Extracting document %s"%document['id'] )
                count += 1
                yield document, corpus
        except StopIteration:
            _logger.debug("Finished walking %d documents"%count)
            return

    def extract_file(self, path, format):
        # instance of the counter
        corporaCounter = Counter()
        # starts the parsing
        fileGenerator = self._walkFile( path, format )
        count=0
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                #self.logger.debug( "%s is extracting document %s (overwrite=%s)"%(tokenizer.TreeBankWordTokenizer.__name__, document['id'], str(overwrite)) )
                # extract filtered ngrams
                docngrams = tokenizer.TreeBankWordTokenizer.extract( \
                    document,\
                    self.stopwords, \
                    self.config['ngramMin'], \
                    self.config['ngramMax'], \
                    self.filters, \
                    self.tagger
                    )
                corporaCounter.add( docngrams, corpusNum )
                count+=1
                if count % 10000 == 0:
                    for period in corporaCounter.index.iterkeys():
                        path = "%d-%s-extractWhitelist.csv"%(count,period)
                        csvfile = Writer("ngram://"+path)
                        csvfile.writeRow( ["status","label","corpus-ngrams w","pos tag","db ID"] )
                        csvfile.export_ngrams(corporaCounter, period)
                        del corporaCounter.index[period]
                    corporaCounter.index={}
        except StopIteration:
            _logger.debug("Finished parsing %d documents"%count)
            return True

    def import_file(self, path, format, overwrite=False):
        # keep duplicate document objects
        self.duplicate = []
        # opens and starts walking a file
        fileGenerator = self._walkFile( path, format )
        # 1st part = ngram extraction
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

        # Second part of file parsing = document graph updating
        except StopIteration:
            # commit changes to indexer
            if self.index is not None:
                self.writer.commit()
            # bottle-neck here on big big corpus
            # inserts/updates corpus and corpora
            self.storage.updateCorpora( self.corpora, overwrite )
            for corpusObj in self.reader.corpusDict.values():
                self.storage.updateCorpus( corpusObj, overwrite )
            self.storage.flushNGramQueue()
            self.storage.ngramindex = []
            self.storage.commitAll()
            return True

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


