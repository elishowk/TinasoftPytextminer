# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# tinasoft core modules
from tinasoft.pytextminer import stopwords, indexer, tagger, tokenizer, corpora, ngram
from tinasoft.data import Engine, Reader, Writer

# checks or creates aaplication directories
from os import makedirs
from os.path import exists, join
if not exists('log'):
    makedirs('log')
if not exists('index'):
    makedirs('index')
if not exists('db'):
    makedirs('db')

# locale management
import locale
# command line utility
from optparse import OptionParser
# configuration file parsing
import yaml

# logger
import logging
import logging.handlers

class TinaApp():
    """ base class for a tinasoft.pytextminer application"""
    def __init__(
        self,
        configFile='config.yaml',
        storage=None,
        loc=None,
        stopw=None,
        index=None):
        # Set up a specific logger with our desired output level
        self.logger = logging.getLogger('TinaAppLogger')
        self.logger.setLevel(logging.DEBUG)

        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            return
        LOG_FILENAME = self.config['log']
        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(\
            LOG_FILENAME, maxBytes=1000000, backupCount=5)
        self.logger.addHandler(handler)
        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            self.logger.error( "locale %s was not found,\
                switching to en_US.UTF-8 by default"%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)

        # load Stopwords object
        if stopw is None:
            self.stopwords = stopwords.StopWords( "file://%s" % self.config['stopwords'] )
        else:
            self.stopwords = stopwords.StopWords( "file://%s" % stopw )

        options = {
            'home' : self.config['dbenv']
        }
        # connection to storage
        if storage is None:
            self.storage = Engine(self.config['storage'], **options)
        else:
            self.storage = Engine(storage)

        # connect to text-index
        if index is None:
            self.index = indexer.TinaIndex(self.config['index'])
        else:
            self.index = indexer.TinaIndex(index)
        self.logger.debug(self.index)
        self.logger.debug( "END OF TinaApp.__init__()")


    def importFile(self,
            path,
            configFile,
            corpora_id,
            overwrite=False,
            index=False,
            ngramize=True,
            format= 'tina'):
        """tina file import method"""
        # import import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the importFile special config : " + exc)
            return
        #self.logger.debug(self.config['fields'])
        dsn = format+"://"+path
        #self.logger.debug(dsn)
        fileReader = Reader(dsn,
            delimiter = self.config['delimiter'],
            quotechar = self.config['quotechar'],
            locale = self.config['locale'],
            fields = self.config['fields']
        )
        self._walkFile(fileReader, corpora_id, overwrite, index, ngramize)

    def _walkFile(self, fileReader, corpora_id, overwrite, index, ngramize):
        """gets importFile() results to insert contents into storage"""
        if index is True:
            writer = self.index.getWriter()
        if ngramize is True:
            self.corpusngrams=[]
        corps = self.storage.loadCorpora(corpora_id)
        if corps is None:
            corps = corpora.Corpora( corpora_id )
        #self.logger.debug(corps)
        fileGenerator = fileReader.parseFile( corps )
        docEdges={}
        try:
            while 1:
                document, corpusNum = fileGenerator.next()
                # insert doc in storage
                self.storage.insertDocument( document )
                if index is True:
                    res = self.index.write(document, writer, overwrite)
                    if res is not None:
                        self.logger.debug("document will not be overwritten")
                if ngramize is True:
                    self.extractNGrams( document, corpusNum,\
                        self.config['ngramMin'], self.config['ngramMax'])
                #Document-corpus Association are included in the object
                if document['id'] in docEdges:
                    docEdges[document['id']]+=1
                else:
                    docEdges[document['id']]=1

        except StopIteration, stop:
            # commit changes to indexer
            writer.commit()
            # insert or updates corpora
            corps = fileReader.corpora
            self.storage.insertCorpora( corps )
            for corpusNum in corps['content']:
                # get the Corpus object and import
                corpus = fileReader.corpusDict[ corpusNum ]
                for doc, occ in docEdges:
                    corpus.addEdge( 'Document', doc, occ )
                for ng in self.corpusngrams:
                    corpus.addEdge( 'NGram', ng['id'], ng['occs'] )
                self.storage.insertCorpus( corpus )
                self.storage.insertAssocCorpus( corpus['id'], corps['id'] )
                del corpus
                del fileReader.corpusDict[ corpusNum ]
            return

    def indexDocuments(self, fileReader):
        raise NotImplemented

    def extractNGrams(self, document, corpusNum, ngramMin, ngramMax):
        """"Main NLP for a document"""
        self.logger.debug(tokenizer.TreeBankWordTokenizer.__name__+\
            " is working on document "+ document['id'])
        docngrams = tokenizer.TreeBankWordTokenizer.extract( document,\
            self.stopwords, ngramMin, ngramMax )
        #self.logger.debug(docngrams)
        for ngid, ng in docngrams.iteritems():
            # save doc occs and delete it
            docOccs = ng['occs']
            del ng['occs']
            loadNG = self.storage.loadNGram( ng['id'] )
            self.corpusngrams+=[ng]
            if loadNG is None:
                # insert a new NGram and updates the corpusngrams index
                self.storage.insertNGram( ng )
            else:
                # updates NGram edges
                restoreNG=ngram.NGram(loadNG)
                self.logger.debug(restoreNG)
                restoreNG.addEdge( 'Document', document['id'], docOccs )
                restoreNG.addEdge( 'Corpus', corpusNum, 1 )
                self.storage.insertNGram( loadNG )
            #self.storage.insertAssocNGramCorpus( ng['id'], corpusNum, self.corpusngrams[ ngid ]['occs'] )
        # clean tokens before ending
        #document['content'] = ""
        document['tokens'] = []
        return docngrams.keys()

    def createCorpus(self):
        raise NotImplemented

    def createCorpora(self):
        raise NotImplemented

    def createDocument(self):
        raise NotImplemented

    def createNGram(self):
        raise NotImplemented

    def writeCooc(self):
        raise NotImplemented
