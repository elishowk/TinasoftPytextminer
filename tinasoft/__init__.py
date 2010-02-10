# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# tinasoft core modules
from tinasoft.pytextminer import stopwords, indexer, tagger, tokenizer, corpora
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
    def __init__(self, configFile='config.yaml', storage=None, loc=None, stopw=None, index=None ):
        # Set up a specific logger with our desired output level
        self.logger = logging.getLogger('TinaAppLogger')
        self.logger.setLevel(logging.DEBUG)

        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the config.yaml file : " + exc)
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

    def importFile(self,
            path,
            configFile,
            corpora_id,
            #minSize=None,
            #maxSize=None,
            format= 'tina'):
        """tina file import method"""
        # import import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the importFile special config : " + exc)
            return
        self.logger.debug(self.config['fields'])
        dsn = format+"://"+path
        self.logger.debug(dsn)
        fileReader = Reader(dsn,
            #minSize = minSize,
            #maxSize = maxSize,
            delimiter = self.config['delimiter'],
            quotechar = self.config['quotechar'],
            locale = self.config['locale'],
            fields = self.config['fields']
        )
        self._walkFile( fileReader, corpora_id )

    def _walkFile(self, fileReader, corpora_id):
        """gets importFile() results to insert contents into storage"""
        corps = self.storage.loadCorpora(corpora_id)
        if corps is None:
            corps = corpora.Corpora( corpora_id )
        # parse the file
        self.logger.debug(corps)
        corps = fileReader.corpora( corps )
        # insert or updates corpora
        self.storage.insertCorpora( corps )
        self.logger.debug("stored a new corpora : " + corps['id'])
        for corpusNum in corps['content']:
            # get the Corpus object and import
            corpus = fileReader.corpusDict[ corpusNum ]
            self.logger.debug(corpus)
            # TODO _walkDocuments TODO TODO
            #fileReader.docDict = fileReader.importCorpus( corpus, \
            #    corpusNum, tokenizer.TreeBankWordTokenizer,\
            #    tagger.TreeBankPosTagger, self.stopwords,\
            #    corps, self.fileReader.docDict )
            del corpus
            del fileReader.corpusDict[ corpusNum ]

    def indexDocuments(self, fileReader):
        raise NotImplemented

    def extractNGrams(self):
        raise NotImplemented

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
