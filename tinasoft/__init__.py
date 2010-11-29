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

__all__ = ["pytextminer","data", "file"]

# python utility modules
import sys
import traceback
from os.path import exists
from os.path import join
from os.path import abspath

import yaml
from datetime import datetime

import locale
import logging
import logging.handlers
import re
# tinasoft core modules
from tinasoft.fileapi import PytextminerFileApi
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
from tinasoft.pytextminer import filtering

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

# default log file
LOG_FILE = "tinasoft-log.txt"

class PytextminerFlowApi(PytextminerFileApi):
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
        self.opened_storage = {}
        # import config yaml to self.config
        try:
            self.config = yaml.safe_load( file( configFilePath, 'rU' ) )
        except yaml.YAMLError, exc:
            print exc
            print "bad config file path passed to PytextminerFlowApi : %s"%configFilePath
            return self.STATUS_ERROR
        # creates applications directories
        self.user = self._init_user_directory()
        self.source_file_directory = self._init_source_file_directory()
        self._init_db_directory()

        self.set_logger(custom_logger)
        # tries to support the locale of the host system or empties it
        try:
            self.locale = self.config['general']['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning(
                "configured locale not found, switching to default ='%s'"%self.locale
            )
            locale.setlocale(locale.LC_ALL, self.locale)

        self.logger.debug("Pytextminer started")

    def __del__(self):
        """resumes the storage transactions when destroying this object"""
        del self.opened_storage

    def set_logger(self, custom_logger=None):
        """
        sets a customized or a default logger
        """
        # Set up a specific logger with our desired output level
        self.LOG_FILE = LOG_FILE
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
            "%(asctime)s - %(levelname)s - %(message)s",
            '%Y-%m-%d %H:%M:%S'
        )
        rotatingFileHandler = logging.handlers.RotatingFileHandler(
            filename = self.LOG_FILE,
            maxBytes = 1024000,
            backupCount = 3
        )
        rotatingFileHandler.setFormatter(formatter)
        logger.addHandler(rotatingFileHandler)
        self.logger = logger

    def get_storage( self, dataset_id, create=True, **options ):
        """
        unique DB connection handler
        one separate database per dataset=corpora
        always check storage is notad before using it
        """
        if dataset_id in self.opened_storage:
            return self.opened_storage[dataset_id]
        try:
            storagedir = join(
                self.config['general']['basedirectory'],
                self.config['general']['dbenv'],
                dataset_id
            )
            options['home'] = storagedir
            if not exists( storagedir ) and create is False:
                raise Exception("dataset DB %s does not exists, won't create it"%dataset_id)
                return self.STATUS_ERROR
            else:
                storage = Engine(self.STORAGE_DSN, **options)
                self.opened_storage[dataset_id] = storage
                self.logger.debug(
                    "new storage connection for data set %s at %s"%(dataset_id, storagedir)
                )
                return storage
        except Exception, exception:
            self.logger.error( exception )
            return self.STATUS_ERROR

    def extract_file(self,
            path,
            dataset,
            whitelistlabel=None,
            outpath=None,
            format='tinacsv',
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
            yield self.STATUS_ERROR
            return
        storage = self.get_storage( dataset )
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return
        # prepares extraction export path
        if outpath is None:
            if whitelistlabel is None:
                whitelistlabel = dataset
            outpath = self._get_user_filepath(
                dataset,
                'whitelist',
                "%s-extract_dataset.csv"%whitelistlabel
            )
        corporaObj = corpora.Corpora(dataset)
        filters = []
        filters += [filtering.PosTagValid(
            config = {
                'rules': re.compile(self.config['datasets']['postag_valid'])
            }
        )]
        filters += [stopwords.StopWords(
            "file://%s"%join(
                self.config['general']['basedirectory'],
                self.config['general']['shared'],
                self.config['general']['stopwords']
            )
        )]
        filters += self._import_userstopwords( userstopwords )
        # instanciate extractor class
        extract = extractor.Extractor(
            storage,
            self.config['datasets'],
            corporaObj,
            filters=filters,
            stemmer=stemmer.Nltk()
        )
        extratorGenerator = extract.extract_file( path, format, outpath, whitelistlabel, minoccs )
        absolute_outpath = abspath(outpath)
        try:
            while 1:
                extratorGenerator.next()
                yield self.STATUS_RUNNING
        except StopIteration, si:
            yield absolute_outpath
            return
        except Exception, ex:
            self.logger.error(traceback.format_exc())
            yield self.STATUS_ERROR
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
            corporaObj = corpora.Corpora(dataset)
            whitelist = self._import_whitelist(whitelistpath)

            storage = self.get_storage( dataset )
            if storage == self.STATUS_ERROR:
                yield self.STATUS_ERROR
                return
            extract = extractor.Extractor(
                storage,
                self.config['datasets'],
                corporaObj,
                filters=None,
                stemmer=stemmer.Identity()
            )
            extractorGenerator = extract.index_file(
                path,
                format,
                whitelist,
                overwrite
            )
            while 1:
                extractorGenerator.next()
                yield self.STATUS_RUNNING
        except IOError, ioe:
            self.logger.error(ioe)
            return
        except StopIteration, si:
            yield extract.duplicate
            return
        except Exception, exc:
            self.logger.error("%s"%exc)
            self.logger.error(traceback.format_exc())
            yield self.STATUS_ERROR
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
        if type(periods) != 'list':
            periods = [periods]
        if not documentgraphconfig: documentgraphconfig = {}
        if not ngramgraphconfig: ngramgraphconfig = {}

        storage = self.get_storage( dataset )
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return

        # outpath is an optional label but transformed to an absolute file path
        params_string = "%s_%s"%(self._get_filepath_id(whitelistpath),"+".join(periods))
        if outpath is None:
            outpath = self._get_user_filepath(dataset, 'gexf', "%s-graph"%params_string)
        else:
            outpath = self._get_user_filepath(dataset, 'gexf',  "%s_%s-graph"%(params_string, outpath) )
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

        GEXFWriter.new_graph( storage, graphmeta )

        periods_to_process = []
        ngram_index = set([])
        doc_index = set([])

        # checks periods and construct nodes' indices
        for period in periods:
            corpus = storage.loadCorpus( period )
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
                ngram_args = ( self.config, storage, process_period, ngramconfig, ngram_index, whitelist )
                adj = graph.NgramGraph( *ngram_args )
                adj.diagonal(ngram_matrix_reducer)
                try:
                    ngram_adj_gen = graph.ngram_submatrix_task( *ngram_args )
                    while 1:
                        ngram_matrix_reducer.add( ngram_adj_gen.next() )
                        yield self.STATUS_RUNNING
                except StopIteration, si:
                    self.logger.debug("NGram matrix reduced for period %s"%process_period)
            load_subgraph_gen = GEXFWriter.load_subgraph( 'NGram', ngram_matrix_reducer, subgraphconfig = ngramconfig)
            try:
                while 1:
                    load_subgraph_gen.next()
                    yield self.STATUS_RUNNING
            except StopIteration, si:
                del ngram_matrix_reducer
        else:
            self.logger.warning("NGram sub-graph not generated because there was no NGrams")

        if len(doc_index) != 0:
            doc_matrix_reducer = graph.MatrixReducerFilter( doc_index )
            for process_period in periods_to_process:
                doc_args = ( self.config, storage, process_period, documentconfig, doc_index, whitelist )
                adj = graph.DocGraph( *doc_args )
                adj.diagonal(doc_matrix_reducer)
                try:
                    doc_adj_gen = graph.document_submatrix_task( *doc_args )
                    while 1:
                        doc_matrix_reducer.add( doc_adj_gen.next() )
                        yield self.STATUS_RUNNING
                except StopIteration, si:
                    self.logger.debug("Document matrix reduced for period %s"%process_period)

            load_subgraph_gen = GEXFWriter.load_subgraph( 'Document',  doc_matrix_reducer, subgraphconfig = documentconfig)
            try:
                while 1:
                    load_subgraph_gen.next()
                    yield self.STATUS_RUNNING
            except StopIteration, si:
                del doc_matrix_reducer

        else:
            self.logger.warning("Document sub-graph not generated because there was no Documents")
        if exportedges is True:
            self.logger.warning("exporting the full graph to current.gexf")
            GEXFWriter.finalize("current.gexf", exportedges=True)
        # returns the absolute path of outpath
        GEXFWriter.finalize(outpath, exportedges=False)
        yield outpath
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
            yield self.STATUS_ERROR
            return
        storage = self.get_storage( dataset )
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return

        corporaObj = corpora.Corpora(dataset)
        whitelist = self._import_whitelist(whitelistpath)
        # prepares extraction export path
        if outpath is not None:
            outpath = self._get_user_filepath(
                dataset,
                'cooccurrences',
                "%s-index_archive.csv"%(whitelist['label'])
            )
            exporter = Writer("coocmatrix://"+outpath)
        else:
            exporter = None
        archive = Reader( format + "://" + path, **self.config['datasets'] )
        archive_walker = archive.walkArchive(periods)
        try:
            period_gen, period = archive_walker.next()
            sc = indexer.ArchiveCounter(storage)
            walkCorpusGen = sc.walkCorpus(whitelist, period_gen, period, exporter, minCooc)
            try:
                while 1:
                    yield walkCorpusGen.next()
            except StopIteration:
                pass
            writeMatrixGen = sc.writeMatrix(period, True, minCooc)
            try:
                while 1:
                    yield writeMatrixGen.next()
            except StopIteration, si:
                pass
        except StopIteration, si:
            yield self.STATUS_OK
            return

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
        storage = self.get_storage( dataset )
        if storage == self.STATUS_ERROR:
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
        return exporter.export_from_storage( storage, periods, whitelist )

    def _import_whitelist(
            self,
            whitelistpath,
            dataset = None,
            userstopwords = None,
            **kwargs
        ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        whitelist_id = self._get_filepath_id(whitelistpath)
        if whitelist_id is not None:
            # whitelistpath EXISTS
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file(stemmer.Nltk())
        # OBSOLETE : TO CHECK
        elif dataset is not None:
            # whitelistpath is a whitelist label into storage
            self.logger.debug("loading whitelist %s from storage"%whitelist_id)
            new_wl = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = whitelistdata.load_from_storage(new_wl, dataset, periods, userstopwords)
        elif exists(whitelistpath):
            whitelist_id = dataset
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file(stemmer.Nltk())
        else:
            raise Exception("unable to find a whitelist at %s"%whitelistpath)
        # TODO stores the whitelist ?
        return new_wl

    def _import_userstopwords(
            self,
            path=None
        ):
        """
        imports a user defined stopwords file (one ngram per line)
        returns a list of one StopWordFilter class instance
        to be used as input of other methods
        """
        if path is None:
            path = join(
                self.config['general']['basedirectory'],
                self.config['general']['userstopwords']
            )
        return [stopwords.StopWordFilter( "file://%s" % path )]



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

    def extract_file(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).extract_file(*arg, **kwargs)
        return self._eraseFlow( generator )

    def index_file(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).index_file(*arg, **kwargs)
        return self._eraseFlow( generator )

    def generate_graph(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).generate_graph(*arg, **kwargs)
        return self._eraseFlow( generator )

    def index_archive(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).index_archive(*arg, **kwargs)
        return self._eraseFlow( generator )


def import_module(name):
    """returns a imported module given its string name"""
    try:
        module =  __import__(name)
        return sys.modules[name]
    except ImportError, exc:
        raise Exception("couldn't load module %s: %s"%(name,exc))
