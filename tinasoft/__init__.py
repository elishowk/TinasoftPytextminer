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
__version__="1.0alpha6"
__url__="http://tinasoft.eu"
__longdescr__="A text-mining python module producing bottom-up thematic field recontruction"
__license__="GNU General Public License"
__keywords__="nlp,textmining,graph"
__maintainer__="elishowk@nonutc.fr"
__maintainer_email__="elishowk@nonutc.fr"
__author__="elishowk@nonutc.fr"
__author__="elishowk@nonutc.fr"
__classifiers__=""
__all__ = ["pytextminer","data"]

# python utility modules
import os
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
from tinasoft.pytextminer import whitelist

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


class TinaApp(object):
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
        loc=None,
        index=None,
        loglevel=logging.DEBUG
        ):
        """
        Initiate config.yaml, logger, locale, storage and index
        """
        object.__init__(self)
        # import config yaml to self.config
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            print exc
            return self.STATUS_ERROR
        # creates app directories
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] ))
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['index'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['index'] ))
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['user'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['user'] ))
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['log'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['log'] ))

        # Set up a specific logger with our desired output level
        self.LOG_FILENAME = join( self.config['general']['basedirectory'], self.config['general']['log'], 'tinasoft.log' )
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
        #try:
        #    if loc is None:
        #        self.locale = self.config['general']['locale']
        #    else:
        #        self.locale = loc
        #    locale.setlocale(locale.LC_ALL, self.locale)
        #except:
        #    self.locale = ''
        #    self.logger.warning( "locale %s was not found,\
        #        switching to default = "%self.locale)
        #    locale.setlocale(locale.LC_ALL, self.locale)

        self.last_dataset_id = None
        self.storage = None
        #option s = {
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
        self.logger.debug("TinaApp started components = config, logger loaded")

    def __del__(self):
        """resumes the storage transactions when destroying this object"""
        del self.storage

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

    def set_storage( self, dataset_id, **options ):
        """
        connection to the dataset's DB
        always check self.storage is not None before using it
        """
        if self.last_dataset_id is not None and self.last_dataset_id == dataset_id:
            # connection already opened
            return
        else:
            if self.storage is not None:
                # safely close the connection
                del self.storage
            self.last_dataset_id = dataset_id
        try:
            storagedir = join( self.config['general']['basedirectory'], self.config['general']['dbenv'], dataset_id )
            if not exists( storagedir ):
                makedirs( storagedir )
            # overwrite db home dir
            options['home'] = storagedir
            self.storage = Engine(self.config['general']['storage'], **options)
        except Exception, exception:
            self.logger.error( exception )
            self.storage = self.last_dataset_id = None


    def extract_file(self,
            path,
            corpora_id,
            index=False,
            format='tinacsv',
            overwrite=False
        ):
        """
        tinasoft source extraction controler
        send a corpora and a storage handler to an Extractor() instance
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
            format= 'tinacsv',
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


    def export_whitelist( self, periods, corporaid, new_whitelist_label, synthesispath=None, compl_whitelist=None, userfilters=None, ngramlimit=65000, minOccs=1, **kwargs):
        """Public access to tinasoft.data.ngram.export_whitelist()"""
        if synthesispath is None:
            synthesispath = join( self.config['general']['user'], "export.csv" )
        self.set_storage( corporaid )
        if self.storage is None:
            return self.STATUS_ERROR
        exporter = Writer('whitelist://'+synthesispath, **kwargs)
        if exporter.export_whitelist( self.storage, periods, new_whitelist_label, userfilters, compl_whitelist, ngramlimit, minOccs ) is not None:
            return self.STATUS_OK
        else:
            return self.STATUS_ERROR

    def import_whitelist( self, filepath, wllabel, **kwargs ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        if isinstance(filepath,str) or isinstance(filepath, unicode):
            filepath=[filepath]
        # new whitelist
        new_wl = whitelist.Whitelist( wllabel, None, wllabel )
        # whitelists aggregation
        for path in filepath:
            wlimport = Reader('whitelist://'+path, **kwargs)
            wlimport.whitelist = new_wl
            new_wl = wlimport.parse_file()
        # TODO stores the whitelist ?
        return new_wl

    def process_cooc( self, whitelist, corporaid, periods, userfilters, **kwargs ):
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
        exporter = Writer('whitelist://'+path, **kwargs)
        return exporter.export_cooc( self.storage, periods, whitelist )

    def get_dataset(self, corporaid):
        """
        Part of the Storage API
        """
        return self.storage.loadCorpora(corporaid)

    def get_corpus(self, corpusid):
        """
        Part of the Storage API
        """
        return self.storage.loadCorpus(corpusid)

    def get_document(self, documentid):
        """
        Part of the Storage API
        """
        return self.storage.loadDocument(documentid)

    def get_ngram(self, ngramid):
        """
        Part of the Storage API
        """
        return self.storage.loadNGram(ngramid)

    def get_datasets(self, *args, **kwargs):
        """
        Request TinaApp File API
        returns list of user's data sets
        """
        dbdir = join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )
        try:
            alldirs = os.listdir(dbdir)
        except:
            return self.STATUS_ERROR
        else:
            valid_dirs = [ds for ds in alldirs if exists( join( dbdir, ds, self.config['general']['storage'].split("://")[1] ))]
            return valid_dirs

    def walk_graph_path( self, corporaid ):
        """returns the list of files in the gexf directory tree"""
        path = join( self.config['user'], corporaid )
        if not exists( path ):
            return []
        return [join( path, file ) for file in os.listdir( path )]

    def _get_graph_path(self, corporaid, periods, threshold=[0.0,1.0]):
        """returns the relative path for a given graph in the graph dir tree"""
        path = join( self.config['user'], corporaid )
        if not exists( path ):
            makedirs( path )
        filename = "-".join( periods ) + "_" \
            + "-".join( map(str,threshold) ) \
            + ".gexf"
        #self.logger.debug( join( path, filename ) )
        return join( path, filename )
