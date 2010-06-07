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
__author__="elias showk"
__author_email__="elishowk@nonutc.fr"
__classifiers__="nlp textmining http"
__all__ = ["pytextminer","data"]

# python utility modules
import os
from os.path import exists
from os.path import join
from os import makedirs
import yaml
from datetime import datetime


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
from tinasoft.pytextminer import indexer
from tinasoft.pytextminer import stopwords

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
        self.user = join( self.config['general']['basedirectory'], self.config['general']['user'] )
        if not exists(self.user):
            makedirs(self.user)
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] ))
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['index'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['index'] ))
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
        #formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        #rotatingFileHandler.setFormatter(formatter)
        self.logger = logging.getLogger('TinaAppLogger')

        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['general']['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning( "locale %s was not found, switching to default = "%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)

        self.last_dataset_id = None
        self.storage = None

        # connect to text-indexer
        if index is None:
            self.index = indexer.TinaIndex(join( self.config['general']['basedirectory'], self.config['general']['index'] ))
        else:
            self.index = indexer.TinaIndex(index)
        self.logger.debug("TinaApp started components = config, logger, locale loaded")

    def __del__(self):
        """resumes the storage transactions when destroying this object"""
        del self.storage

    def set_storage( self, dataset_id, **options ):
        """
        connection to the dataset's DB
        always check self.storage is not None before using it
        """
        if self.last_dataset_id is not None and self.last_dataset_id == dataset_id:
            # connection already opened
            return
        if self.storage is not None:
            self.logger.debug("safely closing last storage connection")
            del self.storage
        self.last_dataset_id = dataset_id
        try:
            storagedir = join( self.config['general']['basedirectory'], self.config['general']['dbenv'], dataset_id )
            if not exists( storagedir ):
                makedirs( storagedir )
            # overwrite db home dir
            options['home'] = storagedir
            self.logger.debug("new connection to a storage for data set %s"%dataset_id)
            self.storage = Engine(self.config['general']['storage'], **options)
        except Exception, exception:
            self.logger.error( exception )
            self.storage = self.last_dataset_id = None

    def extract_file(self,
            path,
            dataset,
            outpath=None,
            index=False,
            format='tinacsv',
            overwrite=False,
            ngramlimit=65000,
            minoccs=1
        ):
        """
        tinasoft source extraction controler
        send a corpora and a storage handler to an Extractor() instance
        """
        # prepares extraction export path
        if outpath is None:
            outpath = self.get_user_path(dataset, 'whitelist', 'extract_file.csv')
        self.logger.debug( "extract_file to %s"%outpath )
        # sends indexer to the file parser
        if index is True:
            index = self.index
        else:
            index = None
        corporaObj = corpora.Corpora(dataset)
        self.set_storage( dataset )
        if self.storage is None:
            return self.STATUS_ERROR
        # instanciate extractor class
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, index )
        return extract.extract_file( path, format, outpath )

    def import_file(self,
            path,
            dataset,
            index=False,
            format='tinacsv',
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

        corporaObj = corpora.Corpora(dataset)
        self.set_storage( dataset )
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


    def export_whitelist( self,
            periods,
            dataset,
            whitelistlabel,
            outpath=None,
            whitelist=None,
            userstopwords=None,
            ngramlimit=65000,
            minoccs=1,
            **kwargs
        ):
        """Public access to tinasoft.data.ngram.export_whitelist()"""
        # creating default outpath
        if outpath is None:
            outpath = self.get_user_path(dataset, 'whitelist', "%s_export_whitelist.csv"%"-".join(periods))
        self.set_storage( dataset )
        if self.storage is None:
            return self.STATUS_ERROR
        exporter = Writer('whitelist://'+outpath, **kwargs)
        return exporter.export_whitelist(
                self.storage,
                periods,
                whitelistlabel,
                userstopwords,
                whitelist,
                ngramlimit,
                minoccs )

    @staticmethod
    def import_whitelist(
            path,
            whitelistlabel,
            **kwargs
        ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        if isinstance(path,str) or isinstance(path, unicode):
            path=[path]
        # new whitelist
        new_wl = whitelist.Whitelist( whitelistlabel, None, whitelistlabel )
        # whitelists aggregation
        for file in path:
            wlimport = Reader('whitelist://'+file, **kwargs)
            wlimport.whitelist = new_wl
            new_wl = wlimport.parse_file()
        # TODO stores the whitelist ?
        return new_wl

    @staticmethod
    def import_userstopwords(
            path=None
        ):
        if path is None:
            path = self.tinasoft.config['datasets']['userstopwords']
        return [stopwords.StopWordFilter( "file://%s" % path )]

    def process_cooc(self,
            dataset,
            periods,
            whitelist,
            userstopwords=None
        ):
        """
        Main function importing a whitelist and generating cooccurrences
        process cooccurrences for each period=corpus
        """
        #self.logger.debug( "entering process_cooc with %d ngrams"%len(whitelist.keys()) )
        self.set_storage( dataset )
        if self.storage is None:
            return self.STATUS_ERROR
        for id in periods:
            try:
                cooc = cooccurrences.MapReduce(self.storage, whitelist, \
                corpusid=id, filter=userstopwords )
            except Warning, warner:
                self.logger.warning( warner )
                continue
            if cooc.walkCorpus() is False:
                return self.STATUS_ERROR
            if cooc.writeMatrix(True) is False:
                return self.STATUS_ERROR
            del cooc
        return self.STATUS_OK


    def export_cooc(self, periods, whitelist, outpath=None):
        """
        returns a text file outpath containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        if outpath is None:
            outpath = self.get_user_path("", 'cooccurrences', 'export_cooc.txt')
        self.logger.debug("export_cooc to %s"%outpath)
        exporter = Writer('coocmatrix://'+outpath)
        return exporter.export_cooc( self.storage, periods, whitelist )

    def export_graph( self,
            dataset,
            periods,
            outpath=None,
            whitelist=None
        ):
        """
        Produces the default GEXF graph file
        for given list of periods (to aggregate)
        and a given ngram whitelist
        """
        if outpath is None:
            outpath = self.get_user_path(dataset, 'gexf', 'export_graph.gexf')
        self.logger.debug("export_graph to %s"%outpath)

        GEXFWriter = Writer('gexf://', **self.config['datamining'])

        self.set_storage( dataset )
        if self.storage is None:
            return self.STATUS_ERROR

        return GEXFWriter.ngramDocGraph(
            outpath,
            db = self.storage,
            periods = periods,
            whitelist = whitelist,
        )

    def get_user_path(self, dataset, filetype, filename):
        """returns a filename from the user directory"""
        path = join( self.user, dataset, filetype )
        now = "_".join(str(datetime.utcnow()).split(" "))
        filename = now + "_" + filename
        if not exists(path):
            makedirs(path)
            return join( path, filename )
        if exists(path,filename):
            return join(path,filename)


    def walk_user_path(self, dataset, filetype):
        """
        Part of the File API
        returns the list of files in the gexf directory tree
        """
        path = join( self.user, dataset, filetype )
        if not exists( path ):
            return []
        return [join( path, file ) for file in os.listdir( path )]


    #def get_graph_path(self, dataset, periods, threshold=[0.0,1.0]):
        """
        OBSOLETE
        Part of the Storage API
        returns the relative path for a given graph in the graph dir tree
        """
    #    path = join( self.config['user'], dataset )
    #    if not exists( path ):
    #        makedirs( path )
    #    filename = "-".join( periods ) + "_" \
    #        + "-".join( map(str,threshold) ) \
    #        + ".gexf"
    #    return join( path, filename )
