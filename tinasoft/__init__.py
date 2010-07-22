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

__all__ = ["pytextminer","data"]
import sys
#sys.stdout = open('tinaapp_stdout.log', 'a+b')
#sys.stderr = open('tinaapp_stderr.log', 'a+b')
# python utility modules
import os
from os.path import exists
from os.path import join
from os.path import abspath
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
from tinasoft.pytextminer import stopwords

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

CWD = '.'
STORAGE_DSN = "tinasqlite://tinasoft.sqlite"

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
            configFilePath,
            loc=None,
            loglevel=logging.DEBUG
        ):
        """
        Init config, logger, locale, storage
        """
        object.__init__(self)
        self.last_dataset_id = None
        self.storage = None
        # import config yaml to self.config
        try:
            self.config = yaml.safe_load( file( configFilePath, 'rU' ) )
        except yaml.YAMLError, exc:
            print exc
            print "bad config yaml path passed to TinaApp"
            print configFilePath
            return self.STATUS_ERROR
        # creates applications directories
        self.user = join( self.config['general']['basedirectory'], self.config['general']['user'] )
        if not exists(self.user):
            makedirs(self.user)

        self.source_file_directory = join( self.config['general']['basedirectory'], self.config['general']['source_file_directory'] )
        if not exists(self.source_file_directory):
            makedirs(self.source_file_directory)

        if not exists(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['dbenv'] ))
        #if not exists(join( self.config['general']['basedirectory'], self.config['general']['index'] )):
            #makedirs(join( self.config['general']['basedirectory'], self.config['general']['index'] ))
        if not exists(join( self.config['general']['basedirectory'], self.config['general']['log'] )):
            makedirs(join( self.config['general']['basedirectory'], self.config['general']['log'] ))

        # Set up a specific logger with our desired output level
        self.LOG_FILENAME = join( self.config['general']['basedirectory'], self.config['general']['log'], 'tinasoft.log' )
        # set default level to DEBUG
        if 'loglevel' in self.config['general']:
            loglevel = LEVELS[self.config['general']['loglevel']]
        # logger config
        #logging.basicConfig(
        #    filename = self.LOG_FILENAME,
        #    datefmt = '%Y-%m-%d %H:%M:%S',
        #    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        #)
        self.logger = logging.getLogger('TinaAppLogger')
        self.logger.setLevel(loglevel)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            '%Y-%m-%d %H:%M:%S'
        )
        # Add the log message handler to the logger
        rotatingFileHandler = logging.handlers.RotatingFileHandler(
            filename = self.LOG_FILENAME,
            maxBytes = 1024000,
            backupCount = 3
        )
        rotatingFileHandler.setFormatter(formatter)
        self.logger.addHandler(rotatingFileHandler)

        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['general']['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning( "configured locale was not found, switching to default ='%s'"%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)

        self.logger.debug("TinaApp started components = config, logger, locale loaded")

    def __del__(self):
        """resumes the storage transactions when destroying this object"""
        if self.storage is not None:
            del self.storage

    def set_storage( self, dataset_id, create=True, **options ):
        """
        unique DB connection handler
        one separate database per dataset=corpora
        always check self.storage is not None before using it
        """
        if self.last_dataset_id is not None and self.last_dataset_id == dataset_id:
            # connection already opened
            return TinaApp.STATUS_OK
        if self.storage is not None:
            self.logger.debug("safely closing last storage connection")
            del self.storage
        try:
            storagedir = join( self.config['general']['basedirectory'], self.config['general']['dbenv'], dataset_id )
            if not exists( storagedir ) and create is False:
                raise Exception("dataset %s does not exists, won't create it"%dataset_id)
                return TinaApp.STATUS_OK
                #else:
                #    makedirs( storagedir )
            else:
                # overwrite db home dir
                options['home'] = storagedir
                self.logger.debug("new connection to a storage for data set %s at %s"%(dataset_id, storagedir))
                self.storage = Engine(STORAGE_DSN, **options)
                self.last_dataset_id = dataset_id
                return TinaApp.STATUS_OK
        except Exception, exception:
            self.logger.error( exception )
            self.storage = self.last_dataset_id = None
            return TinaApp.STATUS_ERROR

    def extract_file(self,
            path,
            dataset,
            whitelistlabel=None,
            outpath=None,
            format='tinacsv',
            overwrite=False,
            minoccs=1,
            userstopwords=None
        ):
        """
        tinasoft source extraction controler
        send a corpora and a storage handler to an Extractor() instance
        """
        path = self._get_sourcefile_path(path)
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        # prepares extraction export path
        if outpath is None:
            if whitelistlabel is None:
                whitelistlabel=dataset
            outpath = self._user_filepath(whitelistlabel, 'whitelist', "%s-extract_dataset.csv"%dataset)
        corporaObj = corpora.Corpora(dataset)
        # instanciate extractor class
        stopwds = stopwords.StopWords( "file://%s"%join(self.config['general']['basedirectory'],self.config['general']['shared'],self.config['general']['stopwords']) )
        userstopwords = self.import_userstopwords(userstopwords)
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, stopwds, userstopwords)
        outpath= extract.extract_file( path, format, outpath, whitelistlabel, minoccs )
        if outpath is not False:
            return abspath(outpath)
        else:
            return self.STATUS_ERROR

    def index_file(self,
            path,
            dataset,
            whitelistpath,
            format='tinacsv',
            overwrite=False,
        ):
        """
        Like import_file but limited to a given whitelist
        """
        path = self._get_sourcefile_path(path)
        corporaObj = corpora.Corpora(dataset)
        whitelist = self.import_whitelist(whitelistpath)
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        # instanciate stopwords and extractor class
        stopwds = stopwords.StopWords( "file://%s"%join(self.config['general']['basedirectory'],self.config['general']['shared'],self.config['general']['stopwords']) )
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, stopwds )
        if extract.index_file(
            path,
            format,
            whitelist,
            overwrite
        ) is True:
            return extract.duplicate
        else:
            return self.STATUS_ERROR

    def import_file(self,
            path,
            dataset,
            format='tinacsv',
            overwrite=False,
        ):
        """
        tinasoft common csv file import controler
        """
        path = self._get_sourcefile_path(path)
        corporaObj = corpora.Corpora(dataset)
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        # instanciate stopwords and extractor class
        stopwds = stopwords.StopWords( "file://%s"%join(self.config['general']['basedirectory'],self.config['general']['shared'],self.config['general']['stopwords']) )
        extract = extractor.Extractor( self.storage, self.config['datasets'], corporaObj, stopwds )
        if extract.import_file(
            path,
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
            whitelistpath=None,
            userstopwords=None,
            minoccs=1,
            **kwargs
        ):
        """Public access to tinasoft.data.ngram.export_whitelist()"""
        # creating default outpath
        if outpath is None:
            outpath = self._user_filepath(dataset, 'whitelist', "%s-export_whitelist.csv"%whitelistlabel)
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        userstopwords = self.import_userstopwords(userstopwords)
        whitelist = self.import_whitelist(whitelistpath, userstopwords)
        exporter = Writer('whitelist://'+outpath, **kwargs)
        return abspath(exporter.export_whitelist(
            self.storage,
            periods,
            whitelistlabel,
            userstopwords,
            whitelist,
            minoccs
        ))

    def import_whitelist(
            self,
            whitelistpath,
            userstopwords=None,
            **kwargs
        ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        if whitelistpath is None:
            return None
        #if isinstance(whitelistpath,str) or isinstance(whitelistpath, unicode):
        #    whitelistpath=[whitelistpath]
        # new whitelist
        #new_wl = whitelist.Whitelist( whitelistlabel, whitelistlabel )
        # whitelists aggregation
        whitelist_id = self._get_filepath_id(whitelistpath)
        if whitelist_id is not None:
            # whitelist_id is a file path
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file()
        else:
            # whitelist_id is a whitelist label
            self.logger.debug("loading whitelist %s from storage"%whitelist_id)
            newwl = whitelist.Whitelist( whitelist_id, whitelist_id )
            newwl.load_from_storage(dataset, periods, userstopwords)
        # TODO stores the whitelist ?
        return new_wl

    def import_userstopwords(
            self,
            path=None
        ):
        if path is None:
            path = join(self.config['general']['basedirectory'], self.config['general']['userstopwords'])
        return [stopwords.StopWordFilter( "file://%s" % path )]

    def process_cooc(self,
            dataset,
            periods,
            whitelistpath=None,
            userstopwords=None
        ):
        """
        Main function importing a whitelist and generating cooccurrences
        process cooccurrences for each period=corpus
        """
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        userstopwords = self.import_userstopwords(userstopwords)
        if whitelistpath is not None:
            whitelist = self.import_whitelist(whitelistpath, userstopwords)
        else:
            whitelist = None
        # for each period, processes cocc and stores them
        for id in periods:
            try:
                cooc = cooccurrences.Simple(self.storage, corpusid=id)
            except Warning, warner:
                self.logger.warning( warner )
                continue
            if cooc.walkCorpus() is False:
                return self.STATUS_ERROR
            if cooc.writeMatrix(True) is False:
                return self.STATUS_ERROR
            del cooc
        return self.STATUS_OK


    def export_cooc(self, periods, whitelistpath, outpath=None):
        """
        returns a text file outpath containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        if outpath is None:
            outpath = self._user_filepath("", 'cooccurrences', "%s-export_cooc.txt"%whitelist['id'])
        self.logger.debug("export_cooc to %s"%outpath)
        whitelist = self.import_whitelist(whitelistpath)
        exporter = Writer('coocmatrix://'+outpath)
        return exporter.export_cooc( self.storage, periods, whitelist )

    def export_graph( self,
            dataset,
            periods,
            outpath=None,
            whitelistpath=None
        ):
        """
        Produces the default GEXF graph file
        for given list of periods (to aggregate)
        and a given ngram whitelist
        """
        if outpath is None:
            outpath = self._user_filepath(dataset, 'gexf', "%s-graph.gexf"%"_".join(periods))

        whitelist = self.import_whitelist(whitelistpath)
        GEXFWriter = Writer('gexf://', **self.config['datamining'])

        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR

        return GEXFWriter.ngramDocGraph(
            outpath,
            db = self.storage,
            periods = periods,
            whitelist = whitelist,
        )

    def _user_filepath(self, dataset, filetype, filename):
        """returns a filename from the user directory"""
        path = join( self.user, dataset, filetype )
        now = "".join(str(datetime.now())[:10].split("-"))
        # standar separator in filenames
        filename = now + "-" + filename
        finalpath = join( path, filename )
        if not exists(path):
            makedirs(path)
            return finalpath
        return finalpath

    def _get_filepath_id(self, path):
        """returns the file identifier from a path"""
        if path is None:
            return None
        if not os.path.isfile( path ):
            return None
        (head, tail) = os.path.split(path)
        if tail == "":
            return None
        filename_components = tail.split("-")
        if len(filename_components) == 1:
            return None
        return filename_components[1]

    def walk_user_path(self, dataset, filetype):
        """
        Part of the File API
        returns the list of files in the gexf directory tree
        """
        path = join( self.user, dataset, filetype )
        if not exists( path ):
            return []
        return [join( path, file ) for file in os.listdir( path )]

    def walk_datasets(self):
        """
        Part of the File API
        returns the list of existing databases
        """
        dataset_list = []
        path = join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )
        validation_filename = STORAGE_DSN.split("://")[1]
        if not exists( path ):
            return dataset_list
        for file in os.listdir( path ):
            if exists(join(path, file, validation_filename)):
                dataset_list += [file]
        return dataset_list

    def walk_source_files(self):
        """
        Part of the File API
        returns the list of files in "sources" directory
        """
        path = join( self.config['general']['basedirectory'], self.config['general']['source_file_directory'] )
        if not exists( path ):
            return []
        return os.listdir( path )

    def _get_sourcefile_path(self, filename):
        path = join( self.config['general']['basedirectory'], self.config['general']['source_file_directory'], filename )
        if not exists( path ):
            raise IOError("file named %s was not found in %s"%(filename, join( self.config['general']['basedirectory'], self.config['general']['source_file_directory'])))
            return None
        return path

