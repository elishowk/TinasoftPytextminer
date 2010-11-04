# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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

__author__="elishowk@nonutc.fr"

__all__ = ["pytextminer","data"]

# python utility modules
import sys
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
#from tinasoft.processpool import ProcessPool
from tinasoft.data import Engine, _factory, _check_protocol
from tinasoft.data import Reader
from tinasoft.data import Writer
from tinasoft.data import whitelist as whitelistdata
from tinasoft.pytextminer import corpora
from tinasoft.pytextminer import extractor
from tinasoft.pytextminer import whitelist
from tinasoft.pytextminer import stopwords
from tinasoft.pytextminer import indexer
from tinasoft.pytextminer import graph
from tinasoft.pytextminer import stemmer

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

# type and name of the main database
STORAGE_DSN = "tinasqlite://tinasoft.sqlite"


class PytextminerFlowApi(object):
    """
    Main application class
    should be used in conjunction with ThreadPool()
    see below the standard return codes
    """

    STATUS_RUNNING = 0
    STATUS_ERROR = 666
    STATUS_OK = 1

    def __init__(
            self,
            configFilePath,
            custom_logger=None
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
            print "bad config yaml path passed to TinaApp : %s"%configFilePath
            return self.STATUS_ERROR
        # creates applications directories
        self.user = join( self.config['general']['basedirectory'], self.config['general']['user'] )
        if not exists(self.user):
            makedirs(self.user)

        self.source_file_directory = join(
            self.config['general']['basedirectory'],
            self.config['general']['source_file_directory']
        )
        if not exists(self.source_file_directory):
            makedirs(self.source_file_directory)

        if not exists(join(
                self.config['general']['basedirectory'],
                self.config['general']['dbenv']
            )
            ):
            makedirs(join(
                self.config['general']['basedirectory'],
                self.config['general']['dbenv']
                )
            )

        self.set_logger(custom_logger)
        # tries support of the locale by the host system
        try:
            self.locale = self.config['general']['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning(
                "configured locale not found, switching to default ='%s'"%self.locale
            )
            locale.setlocale(locale.LC_ALL, self.locale)

        self.logger.debug("TinaApp started")

    def __del__(self):
        """resumes the storage transactions when destroying this object"""
        if self.storage is not None:
            del self.storage

    def set_logger(self, custom_logger=None):
        """
        sets a customized or a default logger
        """
        # here's the default logger
        if not exists(join(
                self.config['general']['basedirectory'],
                self.config['general']['log']
            )
            ):
            makedirs(join(
                self.config['general']['basedirectory'],
                self.config['general']['log']
                )
            )
        # Set up a specific logger with our desired output level
        self.LOG_FILENAME = join(
            self.config['general']['basedirectory'],
            self.config['general']['log'],
            'tinasoft.log'
        )
        # set default level to DEBUG
        if 'loglevel' in self.config['general']:
            loglevel = LEVELS[self.config['general']['loglevel']]
        else:
            loglevel = LEVELS['debug']

        # sets a custom_logger if given
        if custom_logger is not None:
            custom_logger.setLevel(loglevel)
            self.logger = custom_logger
            return
        # default logger configuration
        logger = logging.getLogger('TinaAppLogger')
        logger.setLevel(loglevel)
        # sets default formatting and handler
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            '%Y-%m-%d %H:%M:%S'
        )
        rotatingFileHandler = logging.handlers.RotatingFileHandler(
            filename = self.LOG_FILENAME,
            maxBytes = 1024000,
            backupCount = 3
        )
        rotatingFileHandler.setFormatter(formatter)
        logger.addHandler(rotatingFileHandler)
        self.logger= logger

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
            storagedir = join(
                self.config['general']['basedirectory'],
                self.config['general']['dbenv'],
                dataset_id
            )
            options['home'] = storagedir
            if not exists( storagedir ) and create is False:
                raise Exception("dataset DB %s does not exists, won't create it"%dataset_id)
                return TinaApp.STATUS_OK
            else:
                self.storage = Engine(STORAGE_DSN, **options)
                self.last_dataset_id = dataset_id
                self.logger.debug(
                    "new storage connection for data set %s at %s"%(dataset_id, storagedir)
                )
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
        try:
            path = self._get_sourcefile_path(path)
        except IOError, ioe:
            self.logger.error(ioe)
            return
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return
        # prepares extraction export path
        if outpath is None:
            if whitelistlabel is None:
                whitelistlabel=dataset
            outpath = self._get_user_filepath(
                dataset,
                'whitelist',
                "%s-extract_dataset.csv"%whitelistlabel
            )
        corporaObj = corpora.Corpora(dataset)
        stopwds = stopwords.StopWords(
            "file://%s"%join(
                self.config['general']['basedirectory'],
                self.config['general']['shared'],
                self.config['general']['stopwords']
            )
        )

        userstopwords = self._import_userstopwords( userstopwords )
        # instanciate extractor class
        extract = extractor.Extractor(
            self.storage,
            self.config['datasets'],
            corporaObj,
            stopwds,
            userstopwords,
            stemmer=stemmer.Nltk()
        )
        extratorGenerator = extract.extract_file( path, format, outpath, whitelistlabel, minoccs )
        absolute_outpath = abspath(outpath)
        try:
            while 1:
                extratorGenerator.next()
                yield absolute_outpath
        except StopIteration, si:
            return

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
        try:
            path = self._get_sourcefile_path(path)
        except IOError, ioe:
            self.logger.error(ioe)
            return
        corporaObj = corpora.Corpora(dataset)
        whitelist = self._import_whitelist(whitelistpath)
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return
        # instanciate stopwords and extractor class
        stopwds = stopwords.StopWords(
            "file://%s"%join(
            self.config['general']['basedirectory'],
            self.config['general']['shared'],
            self.config['general']['stopwords'])
        )
        #stemmer = self._import_module( self.config['datasets']['stemmer'] )
        extract = extractor.Extractor(
            self.storage,
            self.config['datasets'],
            corporaObj,
            stopwds,
            stemmer=stemmer.Nltk()
        )
        extractorGenerator = extract.index_file(
            path,
            format,
            whitelist,
            overwrite
        )
        try:
            while 1:
                extractorGenerator.next()
                yield extract.duplicate
        except StopIteration, si:
            return

    def generate_graph(self,
            dataset,
            periods,
            whitelistpath,
            outpath=None,
            ngramgraphconfig=None,
            documentgraphconfig=None,
            exportedges=True
        ):
        """
        Generates the proximity matrices from indexed NGrams/Document/Corpus
        given a list of periods and a whitelist
        Then export the corresponding graph to storage and gexf
        optionnaly export the complete graph to a gexf file for use in tinaweb

        @return absolute path to the gexf file
        """
        if not documentgraphconfig: documentgraphconfig={}
        if not ngramgraphconfig: ngramgraphconfig={}

        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return

        # outpath is an optional label but transformed to an absolute file path
        params_string = "%s_%s"%(self._get_filepath_id(whitelistpath),"+".join(periods))
        if outpath is None:
            outpath = self._get_user_filepath(dataset, 'gexf', params_string)
        else:
            outpath = self._get_user_filepath(dataset, 'gexf', params_string + "_%s"%outpath)
        outpath = abspath( outpath + ".gexf" )
        # loads the whitelist
        whitelist = self._import_whitelist(whitelistpath)
        # creates the GEXF exporter
        GEXFWriter = Writer('gexf://', **self.config['datamining'])

        # adds meta to the futur gexf file
        graphmeta = {
            'parameters': {
                'periods' : "+".join(periods),
                'whitelist': self._get_filepath_id(whitelistpath),
                'location': outpath,
                'dataset': dataset,
                'layout/algorithm': 'tinaforce',
                'rendering/edge/shape': 'curve',
                'data/source': 'browser'
            },
            'description': "a tinasoft graph",
            'creators': ["CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)"],
            'date': "%s"%datetime.now().strftime("%Y-%m-%d"),
        }

        GEXFWriter.new_graph( self.storage, graphmeta )

        periods_to_process = []
        ngram_index = set([])
        doc_index = set([])

        # checks periods and construct nodes' indices
        for period in periods:
            corpus = self.storage.loadCorpus( period )
            if corpus is not None:
                periods_to_process += [period]
                # unions
                ngram_index |= set( corpus['edges']['NGram'].keys() )
                doc_index |= set( corpus['edges']['Document'].keys() )
            else:
                self.logger.debug('Period %s not found in database, skipping'%str(period))
        # intersection with the whitelist
        ngram_index &= set( whitelist['edges']['NGram'].keys() )

        # TODO here's the key to replace actuel Matrix index handling
        doc_index = list(doc_index)
        doc_index.sort()
        ngram_index = list(ngram_index)
        ngram_index.sort()

        # updates default config with parameters
        self.config['datamining']['NGramGraph'].update(ngramgraphconfig)
        self.config['datamining']['DocumentGraph'].update(documentgraphconfig)
        ngramconfig = self.config['datamining']['NGramGraph']
        documentconfig = self.config['datamining']['DocumentGraph']

        if len(ngram_index) != 0:
            # hack
            if ngramconfig['proximity']=='cooccurrences':
                ngram_matrix_reducer = graph.MatrixReducerFilter( ngram_index )
            if ngramconfig['proximity']=='pseudoInclusion':
                ngram_matrix_reducer = graph.PseudoInclusionMatrix( ngram_index )
            if ngramconfig['proximity']=='equivalenceIndex':
                ngramconfig['nb_documents'] = len(doc_index)
                ngram_matrix_reducer = graph.EquivalenceIndexMatrix( ngram_index )
            # hack
            ngramconfig['proximity']='cooccurrences'
            for process_period in periods_to_process:
                ngram_args = ( self.config, self.storage, process_period, ngramconfig, ngram_index, whitelist )
                adj = graph.NgramGraph( *ngram_args )
                adj.diagonal(ngram_matrix_reducer)
                try:
                    ngram_adj_gen = graph.ngram_submatrix_task( *ngram_args )
                    while 1:
                        ngram_matrix_reducer.add( ngram_adj_gen.next() )
                        yield None
                except StopIteration, si:
                    self.logger.debug("NGram matrix reduced for period %s"%process_period)
            load_subgraph_gen = GEXFWriter.load_subgraph( 'NGram', ngram_matrix_reducer, subgraphconfig = ngramconfig)
            try:
                while 1:
                    load_subgraph_gen.next()
                    yield outpath
            except StopIteration, si:
                del ngram_matrix_reducer
        else:
            self.logger.warning("NGram sub-graph not generated because there was no NGrams")

        if len(doc_index) != 0:
            doc_matrix_reducer = graph.MatrixReducerFilter( doc_index )
            for process_period in periods_to_process:
                doc_args = ( self.config, self.storage, process_period, documentconfig, doc_index, whitelist )
                adj = graph.DocGraph( *doc_args )
                adj.diagonal(doc_matrix_reducer)
                try:
                    doc_adj_gen = graph.document_submatrix_task( *doc_args )
                    while 1:
                        doc_matrix_reducer.add( doc_adj_gen.next() )
                        yield None
                except StopIteration, si:
                    self.logger.debug("Document matrix reduced for period %s"%process_period)

            load_subgraph_gen = GEXFWriter.load_subgraph( 'Document',  doc_matrix_reducer, subgraphconfig = documentconfig)
            try:
                while 1:
                    load_subgraph_gen.next()
                    yield outpath
            except StopIteration, si:
                del doc_matrix_reducer

        else:
            self.logger.warning("Document sub-graph not generated because there was no Documents")
        if exportedges is True:
            self.logger.warning("exporting the full graph to current.gexf")
            GEXFWriter.finalize("current.gexf", exportedges=True)
        # returns the absolute path of outpath
        GEXFWriter.finalize(outpath, exportedges=False)
        return

    def index_archive(self,
            path,
            dataset,
            periods,
            whitelistpath,
            format,
            outpath=None,
            minCooc=1
        ):
        """
        Index a whitelist against an archive of articles,
        updates cooccurrences into storage,
        optionally exporting cooccurrence matrix
        """
        try:
            # path is an archive directory
            path = self._get_sourcefile_path(path)
        except IOError, ioe:
            self.logger.error(ioe)
            return self.STATUS_ERROR
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR

        corporaObj = corpora.Corpora(dataset)
        whitelist = self._import_whitelist(whitelistpath)
        # prepares extraction export path
        if outpath is not None:
            outpath = self._get_user_filepath(
                dataset,
                'cooccurrences',
                "%s_%s-index_archive.csv"%("+".join(periods),whitelist['label'])
            )
            exporter = Writer("coocmatrix://"+outpath)
        else:
            exporter = None
        archive = Reader( format + "://" + path )
        archive_walker = archive.walkArchive(periods)
        try:
            period_gen, period = archive_walker.next()
            sc = indexer.ArchiveCounter(self.storage)
            if not sc.walkCorpus(whitelist, period_gen, period, exporter, minCooc):
                return self.STATUS_ERROR
            elif not sc.writeMatrix(period, True, minCooc):
                return self.STATUS_ERROR
        except StopIteration, si:
            return self.STATUS_OK

    def export_cooc(self,
            dataset,
            periods,
            whitelistpath=None,
            outpath=None
        ):
        """
        OBSOLETE : uses tinasqlite selectCorpusCooc
        returns a text file outpath containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        if self.set_storage( dataset ) == self.STATUS_ERROR:
            return self.STATUS_ERROR
        if outpath is None:
            outpath = self._get_user_filepath(
                dataset,
                'cooccurrences',
                "%s-export_cooc.txt"%"+".join(periods)
            )
        whitelist = None
        if whitelistpath is not None:
            whitelist = self._import_whitelist(whitelistpath)
        exporter = Writer('coocmatrix://'+outpath)
        return exporter.export_cooc( self.storage, periods, whitelist )

    def _import_whitelist(
            self,
            whitelistpath,
            userstopwords = None,
            **kwargs
        ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        whitelist_id = self._get_filepath_id(whitelistpath)
        if whitelist_id is not None:
            # whitelist_path EXIST
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file()
        else:
            # whitelist_id is a whitelist label into storage
            self.logger.debug("loading whitelist %s from storage"%whitelist_id)
            new_wl = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = whitelistdata.load_from_storage(new_wl, dataset, periods, userstopwords)
        # TODO stores the whitelist ?
        return new_wl

    def _import_userstopwords(
            self,
            path=None
        ):
        """
        imports a user defined stopword file (one ngram per line)
        returns a list of one StopWordFilter class instance
        to be used as input of other methods
        """
        if path is None:
            path = join(
                self.config['general']['basedirectory'],
                self.config['general']['userstopwords']
            )
        return [stopwords.StopWordFilter( "file://%s" % path )]

    def _get_user_filepath(self, dataset, filetype, filename):
        """returns a relative filename into the user directory"""
        path = join( self.user, dataset, filetype )
        now = "".join(str(datetime.now())[:10].split("-"))
        # standard separator in filenames
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
        returns the list of files in the user directory tree
        """
        path = join( self.user, dataset, filetype )
        if not exists( path ):
            yield []
        yield [
            abspath(join( path, file ))
            for file in os.listdir( path )
            if not file.startswith("~") and not file.startswith(".")
        ]

    def walk_datasets(self):
        """
        Part of the File API
        returns the list of existing databases
        """
        dataset_list = []
        path = join( self.config['general']['basedirectory'], self.config['general']['dbenv'] )
        validation_filename = STORAGE_DSN.split("://")[1]
        if not exists( path ):
            yield dataset_list
        for file in os.listdir( path ):
            if exists(join(path, file, validation_filename)):
                dataset_list += [file]
        yield dataset_list

    def walk_source_files(self):
        """
        Part of the File API
        returns the list of files in "sources" directory
        """
        path = join(
            self.config['general']['basedirectory'],
            self.config['general']['source_file_directory']
        )
        if not exists( path ):
            yield []
        yield os.listdir( path )

    def _get_sourcefile_path(self, filename):
        path = join(
            self.config['general']['basedirectory'],
            self.config['general']['source_file_directory'],
            filename
        )
        if not exists( path ):
            raise IOError("file named %s was not found in %s"%(
                filename,
                join(
                    self.config['general']['basedirectory'],
                    self.config['general']['source_file_directory'])
                )
            )
            return None
        return path

    def _import_module(self, name):
        """returns a imported module given its string name"""
        try:
            module =  __import__(name)
            return sys.modules[name]
        except ImportError, exc:
            raise Exception("couldn't load module %s: %s"%(name,exc))

class PytextminerApi(PytextminerFlowApi):
    def _eraseFlow(self, generator):
        """
        Flattens all API generator for simpler scripting of PytextminerApi
        """
        try:
            while 1:
                lastValue = generator.next()
        except StopIteration, si:
            return lastValue

    def extract_file(*arg, **kwargs):
        generator = super(PytextminerFlowApi, self).extract_file(*arg, **kwargs)

    def index_file(*arg, **kwargs):
        generator = super(PytextminerFlowApi, self).index_file(*arg, **kwargs)

    def generate_graph(*arg, **kwargs):
        generator = super(PytextminerFlowApi, self).generate_graph(*arg, **kwargs)
