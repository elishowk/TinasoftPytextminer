# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# tinasoft core modules
from tinasoft.pytextminer import stopwords, indexer
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
LOG_FILENAME = 'log/tinasoft.log'


class TinaApp():
    """ base class for a tinasoft.pytextminer application"""
    def __init__(self, configFile='config.yaml', storage=None, loc=None, stopw=None, index=None ):
        # Set up a specific logger with our desired output level
        self.logger = logging.getLogger('TinaAppLogger')
        self.logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(\
            LOG_FILENAME, maxBytes=1000000, backupCount=5)

        self.logger.addHandler(handler)
        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the config.yaml file : " + exc)
            return

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

        # connect sqlite database
        if storage is None:
            self.storage = Engine(self.config['storage'])
        else:
            self.storage = Engine(storage)

        # connect to text-index
        if index is None:
            self.index = indexer.TinaIndex(self.config['index'])
        else:
            self.index = indexer.TinaIndex(index)

    def importFile(self, path, fields,
            format='tina',
            new_corpora_id=None,
            corpora_id=None,
            minSize=None,
            maxSize=None,
            delimiter = None,
            quotechar = None,
            locale = None):
        """tina csv file import method"""
        dsn = format+"://"+path
        tinaImporter = Reader(dsn,
            minSize = minSize,
            maxSize = maxSize,
            delimiter = delimiter,
            quotechar = quotechar,
            locale = locale,
            fields = fields
        )
        if new_corpora_id is not None:
            corps = corpora.Corpora( new_corpora_id )
        elif corpora_id is not None:
            corps = self.storage.loadCorpora(corpora_id)
        else:
            self.logger.error("importFile failed : new_corpora_id "+\
             "and corpora_id are both None, "+\
             "please submit at least one param")
            return
        corps = tinaImporter.corpora( corps )
        self.storage.insertCorpora( corps['id'], corps )
        tinaextract = Writer(self.options.outdb, locale=self.locale, format="json")
        for corpusNum in corps['content']:
            # get the Corpus object and import
            corpus = tinaImporter.corpusDict[ corpusNum ]
            tinaImporter.docDict = tinaextract.importCorpus( corpus, corpusNum, tokenizer.TreeBankWordTokenizer, tagger.TreeBankPosTagger, self.stopwords, corps, tinaImporter.docDict )
            del corpus
            del tinaImporter.corpusDict[ corpusNum ]

    def indexDocuments(self, docObjList):
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
