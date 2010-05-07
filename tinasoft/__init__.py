# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# python utility modules
from os.path import exists
from os.path import join
from os import makedirs
import yaml

# json encoder for communicate with the outer world
import jsonpickle

import locale
import logging
import logging.handlers

# tinasoft core modules
from tinasoft.data import Engine
from tinasoft.data import Reader
from tinasoft.data import Writer
from tinasoft.pytextminer import corpora
from tinasoft.pytextminer import extractor
from tinasoft.pytextminer import cooccurrences

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


class TinaApp():
    """
    Main application class
    should be used in conjunction with ThreadPool()
    see below the standard return codes
    """

    STATUS_RUNNING = 0
    STATUS_ERROR = 666
    STATUS_OK = 1

    @staticmethod
    def notify( subject, msg, data ):
        """
        This method should be overwritten by a context-dependent notifier
        """
        pass

    def __init__(
        self,
        configFile='config.yaml',
        storage=None,
        loc=None,
        stopw=None,
        index=None,
        loglevel=logging.DEBUG):
        """
        Initiate config.yaml, logger, locale, storage and index
        """
        # import config yaml to self.config
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            print exc
            return self.STATUS_ERROR
        # creates app directories
        if not exists(self.config['general']['dbenv']):
            makedirs(self.config['general']['dbenv'])
        if not exists(self.config['general']['index']):
            makedirs(self.config['general']['index'])
        if not exists(self.config['general']['user']):
            makedirs(self.config['general']['user'])
        if not exists(self.config['general']['log']):
            makedirs(self.config['general']['log'])

        # Set up a specific logger with our desired output level
        self.LOG_FILENAME = join( self.config['general']['log'], 'tinasoft.log' )
        # set default level to DEBUG
        if 'loglevel' in self.config['general']:
            loglevel = LEVELS[self.config['general']['loglevel']]
        # logger config
        logging.basicConfig(
            filename = self.LOG_FILENAME,
            level = loglevel,
            datefmt = '%Y-%m-%d %H:%M:%S',
            format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        # Add the log message handler to the logger
        rotatingFileHandler = logging.handlers.RotatingFileHandler(
            filename = self.LOG_FILENAME,
            maxBytes = 1024,
            backupCount = 3
        )
        # formatting
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        rotatingFileHandler.setFormatter(formatter)
        self.logger = logging.getLogger('TinaAppLogger')

        #self.logger.setLevel(loglevel)
        #self.logger.addHandler(handler)

        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['general']['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning( "locale %s was not found,\
                switching to default = "%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)


        #options = {
        #    'home' : self.config['dbenv']
        #}
        # connection to storage
        #if storage is None:
        #    self.storage = Engine(self.config['storage'], **options)
        #else:
        #    self.storage = Engine(storage)

        # connect to text-indexer
        #if index is None:
        #    self.index = indexer.TinaIndex(self.config['index'])
        #else:
        #    self.index = indexer.TinaIndex(index)
        self.logger.debug("TinaApp started components = config, logger, locale")

    def serialize(self, obj):
        """
        Encoder to send messages to the host appllication
        """
        return jsonpickle.encode(obj)

    def deserialize(self, str):
        """
        Decoder for the host's application messages
        """
        return jsonpickle.decode(str)

    def set_storage( self, dataset_id ):
        """
        connection to the dataset's DB
        always check self.storage is not None before using it
        """
        try:
            storagedir = join( self.config['general']['dbenv'], dataset_id )
            if not exists( storagedir ):
                makedirs( storagedir )
            options = {
                'home' : storagedir
            }
            self.storage = Engine(self.config['general']['storage'], **options)
        except Exception, exception:
            self.logger.error( exception )
            self.storage = None

    def extract_file(self,
            path,
            corpora_id,
            index=False,
            format='tina',
            overwrite=False
        ):
        """
        tinasoft common file extraction controler
        send a corpora and the storage handler to an Extractor() instance
        """

        # sends indexer to the file parser
        if index is True:
            index = self.index
        else:
            index = None
        corporaObj = corpora.Corpora(corpora_id)
        self.set_storage( corpora_id )
        if self.storage is None:
            return self.STATUS_ERROR
        # instanciate extractor class
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, index )

        if extract.extract_file( path, format ) is True:
            return self.STATUS_OK
        else:
            return self.STATUS_ERROR

    def import_file(self,
            path,
            corpora_id,
            index=False,
            format= 'tina',
            overwrite=False,
        ):
        """
        tinasoft common csv file import controler
        initiate the import.yaml config file, default ngram's filters,
        a file Reader() to be sent to the Extractor()
        """
        # sends indexer to the file parser
        if index is True:
            index=self.index
        else:
            index = None

        corporaObj = corpora.Corpora(corpora_id)
        self.set_storage( corpora_id )
        if self.storage is None:
            return self.STATUS_ERROR
        # instanciate extractor class
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, index )
        if extract.import_file( path,
            format,
            overwrite
        ) is True:
            return extract.duplicate
        else:
            return self.STATUS_ERROR

    def export_whitelist( self, periods, corporaid, synthesispath=None, whitelist=None, userfilters=None, **kwargs):
        """Public access to tinasoft.data.ngram.export_whitelist()"""
        if synthesispath is None:
            synthesispath = join( self.config['general']['user'], "export.csv" )
        self.set_storage( corporaid )
        if self.storage is None:
            return self.STATUS_ERROR
        exporter = Writer('ngram://'+synthesispath, **kwargs)
        if exporter.export_whitelist( self.storage, periods, corporaid, userfilters, whitelist ) is not None:
            return self.STATUS_OK
        else:
            return self.STATUS_ERROR

    def get_whitelist( self, filepath, **kwargs ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        if isinstance(filepath,str) or isinstance(filepath, unicode):
            filepath=[filepath]
        whitelist = {}
        # whitelists aggregation
        for path in filepath:
            wlimport = Reader('ngram://'+path, **kwargs)
            wlimport.whitelist = whitelist
            whitelist = wlimport.import_whitelist()
        return whitelist

    def process_cooc ( self, whitelist, corporaid, periods, userfilters, **kwargs ):
        """
        Main function importing a whitelist and generating cooccurrences
        process cooccurrences for each period=corpus
        """
        self.logger.debug( "entering process_cooc with %d ngrams"%len(whitelist.keys()) )
        self.set_storage( corporaid )
        if self.storage is None:
            return self.STATUS_ERROR
        for id in periods:
            try:
                cooc = cooccurrences.MapReduce(self.storage, whitelist, \
                corpusid=id, filter=userfilters )
            except Warning, warner:
                self.logger.warning( warner )
                continue
            if cooc.walkCorpus() is False:
                tinasoft.TinaApp.notify( None,
                    'tinasoft_runProcessCoocGraph_running_status',
                    self.serialize( self.STATUS_ERROR )
                )
            if cooc.writeMatrix(True) is False:
                tinasoft.TinaApp.notify( None,
                    'tinasoft_runProcessCoocGraph_running_status',
                    self.serialize( self.STATUS_ERROR )
                )
            del cooc
        return self.STATUS_OK

    def export_graph( self, path, corporaid, periods, whitelist=None ):
        """
        Produce and write Tinasoft's default GEXF graph
        for given list of periods (to aggregate)
        and a given ngram whitelist
        """
        #opts['template'] = self.config['template']
        GEXFWriter = Writer('gexf://', **self.config['datamining'])
        self.set_storage( corporaid )
        if self.storage is None:
            return self.STATUS_ERROR
        GEXFString = GEXFWriter.ngramDocGraph(
            db = self.storage,
            periods = periods,
            whitelist = whitelist,
        )
        if GEXFString == self.STATUS_ERROR:
            return self.STATUS_ERROR

        TinaApp.notify( None,
            'tinasoft_runProcessCoocGraph_running_status',
            'writing gexf to %s'%path
        )
        open(path, 'w+b').write(GEXFString)
        return path

    def export_cooc(self, path, periods, whitelist, **kwargs):
        """
        returns a text file path containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        exporter = Writer('ngram://'+path, **kwargs)
        return exporter.export_cooc( self.storage, periods, whitelist )

    def getCorpora(self, corporaid):
        """
        Part of the Storage API
        """
        return self.serialize(self.storage.loadCorpora(corporaid))

    def getCorpus(self, corpusid):
        """
        Part of the Storage API
        """
        return self.serialize(self.storage.loadCorpus(corpusid))

    def getDocument(self, documentid):
        """
        Part of the Storage API
        """
        return self.serialize(self.storage.loadDocument(documentid))

    def getNGram(self, ngramid):
        """
        Part of the Storage API
        """
        return self.serialize(self.storage.loadNGram(ngramid))

    def listCorpora(self, default=None):
        """
        Part of the Storage API
        """
        if default is None:
            default=[]
        try:
            select = self.storage.select('Corpora::')
            while 1:
                default += [select.next()[1]]
        except StopIteration:
            return self.serialize( default )
