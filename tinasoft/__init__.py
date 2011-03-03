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

# python core modules
import sys
import traceback
from os.path import exists
from os.path import join
from os.path import abspath
from os.path import split

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
from tinasoft.pytextminer.graph import process_document_subgraph, process_ngram_subgraph
from tinasoft.pytextminer.graph import subgraph, matrix
from tinasoft.pytextminer import stemmer
from tinasoft.pytextminer import tokenizer
from tinasoft.pytextminer import filtering

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

LOG_FILE = "pytextminer-log.txt"

class PytextminerFlowApi(PytextminerFileApi):
    """
    Main application class
    In a Server context, all public methods return python generators (see httpserver.py)
    In a scripting context, better using PyTextMinerApi
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
        Init config, logger, locale, and application directories
        """
        self.opened_storage = {}
        # import config yaml to self.config
        self.config_path = configFilePath
        self._load_config()
        # creates applications directories
        self.user = self._init_user_directory()
        self._init_source_file_directory()
        self._init_db_directory()
        self._init_whitelist_directory()

        self.set_logger(custom_logger)
        # tries to support the locale of the host system or empties it
        try:
            self.locale = self.config['general']['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = ''
            self.logger.warning(
                "locale %s not found on this system, switching to the default"%self.config['general']['locale']
            )
            locale.setlocale(locale.LC_ALL, self.locale)
        self.logger.debug("===Pytextminer started===")

    def _load_config(self):
        # import config yaml to self.config
        self.config = yaml.safe_load( file( self.config_path, 'rU' ) )

    def set_logger(self, custom_logger=None):
        """
        sets a customized or the default logger
        """
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
            filename = LOG_FILE,
            maxBytes = self.config['general']['logsize'],
            backupCount = 2
        )
        rotatingFileHandler.setFormatter(formatter)
        logger.addHandler(rotatingFileHandler)
        self.logger = logger

    def get_storage(self, dataset_id, create=True, drop_tables=False, **options):
        """
        DB connection handler
        one separate database per dataset=corpora
        always check storage is not openend before using it
        """
        if dataset_id in self.opened_storage:
            return self.opened_storage[dataset_id]

        try:
            options['drop_tables'] = drop_tables
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
            whitelistlabel=None,
            outpath=None,
            format='tinacsv',
            minoccs=1,
            userstopwords=None
        ):
        """
        pytextminer's NGrams extraction controler : creates new whitelists of proposed topics
        """
        self._load_config()
        try:
            sourcefilename = path
            path = self._get_sourcefile_path(path)
            if whitelistlabel is None:
                whitelistlabel = split(path)[1]

            # prepares extraction export path
            if outpath is None:
                outpath = self._get_whitelist_filepath(
                    whitelistlabel
                )

            filters = [filtering.PosTagValid(
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

            newwl = whitelist.Whitelist(whitelistlabel, whitelistlabel)

            yield self.STATUS_RUNNING
            corporaObj = corpora.Corpora(whitelistlabel)
            corporaObj.addEdge('Whitelist', newwl['label'], outpath)
            corporaObj.addEdge('Source', sourcefilename, abspath(path))

            extract = extractor.Extractor(
                self.config['datasets'],
                path,
                format,
                newwl.storage,
                corporaObj,
                filters,
                stemmer.Nltk(),
                tokenizer.NGramTokenizer
            )
            extractorGenerator = extract.index()
            
            while 1:
                extractorGenerator.next()
                yield self.STATUS_RUNNING
        except IOError, ioe:
            self.logger.error("%s"%ioe)
            yield self.STATUS_ERROR
            return
        except StopIteration:
            whitelist_exporter = Writer("whitelist://"+outpath)
            whitelist_exporter.write_whitelist(newwl, whitelistlabel, minoccs=minoccs)
            yield abspath(outpath)
            return

    def index_file(self,
            path,
            dataset,
            whitelistpath,
            format='tinacsv',
            overwrite=False,
        ):
        """
        pytextminer's indexation controler : whitelist + source file => session database
        """
        self._load_config()
        try:
            sourcefilename = path
            path = self._get_sourcefile_path(path)

            corporaObj = corpora.Corpora(dataset)
            importedwl = self._import_whitelist(whitelistpath)

            corporaObj.addEdge('Whitelist', importedwl['label'], whitelistpath)
            corporaObj.addEdge('Source', sourcefilename, abspath(path))

            storage = self.get_storage(dataset, create=True, drop_tables=False)
            if storage == self.STATUS_ERROR:
                yield self.STATUS_ERROR
                return

            extract = extractor.Extractor(
                self.config['datasets'],
                path,
                format,
                storage,
                corporaObj,
                [importedwl],
                stemmer.Identity(),
                tokenizer.NLemmaTokenizer,
                whitelist=importedwl
            )
            extractorGenerator = extract.index()
            
            while 1:
                extractorGenerator.next()
                yield self.STATUS_RUNNING
        except IOError, ioe:
            self.logger.error("%s"%ioe)
            yield self.STATUS_ERROR
            raise ioe
            return
        except StopIteration:
            #storage.flushNGramQueue()
            del storage
            preprocessgen = self.graph_preprocess(dataset)
            try:
                while 1:
                    yield preprocessgen.next()
            except StopIteration:
                pass
        yield extract.duplicate
        return

    def _new_graph_writer(self, dataset, periods, whitelistid, storage=None, generate=True, preprocess=False):
        """
        creates the GEXF Graph exporter
        """
        graphwriter = Writer('gexf://', **self.config['datamining'])
        # adds meta to the futur gexf file
        graphmeta = {
            'parameters': {
                'periods' : "+".join(periods),
                'whitelist': whitelistid,
                'dataset': dataset,
                'layout/algorithm': 'tinaforce',
                'rendering/edge/shape': 'curve',
                'data/source': 'browser'
            },
            'description': "a tinasoft graph",
            'creators': ["CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)"],
            'date': "%s"%datetime.now().strftime("%Y-%m-%d"),
            'nodes': {
                'NGram' : {},
                'Document': {}
            }
        }
        graphwriter.new_graph( storage, graphmeta, periods, generate, preprocess)
        return graphwriter

    def graph_preprocess(self, dataset):
        """
        Preprocesses the whole graph database
        specially cooccurrences used for further graph generation
        """
        self.logger.debug("starting graph_preprocess")

        storage = self.get_storage(dataset, create=True, drop_tables=False)
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return

        yield self.STATUS_RUNNING
        ngramgraphconfig = self.config['datamining']['NGramGraph']
        datasetObj = storage.loadCorpora(dataset)

        ### cooccurrences calculated for each period
        for corpusid in datasetObj['edges']['Corpus'].keys():
            period = storage.loadCorpus( corpusid )
            if period is None: continue
            ngram_index = set( period.edges['NGram'].keys() )
            doc_index = set( period.edges['Document'].keys() )
            if len(ngram_index) == 0:
                self.logger.warning("period %s has NO NGram indexed, skipping graph_preprocess"%period.id)
                continue
            if len(doc_index) == 0:
                self.logger.warning("period %s has NO Document indexed, skipping graph_preprocess"%period.id)
                continue

            yield self.STATUS_RUNNING
            cooc_writer = self._new_graph_writer(
                dataset,
                [period['id']],
                "preprocess tmp graph",
                storage,
                generate=False,
                preprocess=True
            )

            yield self.STATUS_RUNNING
            ngram_matrix_reducer = matrix.MatrixReducer(ngram_index)
            ngram_graph_preprocess = _dynamic_get_class("tinasoft.pytextminer.graph.subgraph", "NgramGraphPreprocess")
            ngramsubgraph_gen = process_ngram_subgraph(
                self.config,
                dataset,
                [period],
                ngram_index,
                doc_index,
                ngram_matrix_reducer,
                ngramgraphconfig,
                cooc_writer,
                storage,
                ngram_graph_preprocess
            )

            try:
                while 1:
                    yield self.STATUS_RUNNING
                    ngramsubgraph_gen.next()
            except StopIteration:
                self.logger.debug("exporting master whitelist")

                whitelistlabel = "%s_master"%datasetObj['id']
                outpath = self._get_whitelist_filepath(whitelistlabel)
                # this whitelist == dataset
                newwl = whitelist.Whitelist(whitelistlabel, whitelistlabel)
                # dataset's storage == master whitelist's storage
                del newwl.storage
                newwl.storage = storage
                yield self.STATUS_RUNNING
                # exports the dataset's whitelist
                whitelist_exporter = Writer("whitelist://"+outpath)
                yield abspath( whitelist_exporter.write_whitelist(newwl, datasetObj['id']) )
                return


    def generate_graph(self,
            dataset,
            periods=None,
            outpath=None,
            ngramgraphconfig=None,
            documentgraphconfig=None,
            exportedges=True
        ):
        """
        Generates the graphs from indexed NGrams/Document/Corpus
        given a list of @periods
        Then export the corresponding graph to storage and gexf
        optionnaly exports the complete graph to a gexf file for use in tinaweb

        @return absolute path to the GEXF nodes file
        """
        self._load_config()

            
        if not documentgraphconfig: documentgraphconfig = {}
        if not ngramgraphconfig: ngramgraphconfig = {}

        storage = self.get_storage(dataset, create=False, drop_tables=False)
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return

        if periods is None:
            self.logger.debug("no periods parameters, will use all periods from the dataset")
            corpora = storage.loadCorpora(dataset)

            if corpora is None:
                yield self.STATUS_ERROR
                raise Exception("%s dataset not found in database")
                return
            else:
                periods = corpora['edges']['Corpus'].keys()

        if not isinstance(periods, list):
            periods = [periods]

        # params_string formed with the periods
        if len(periods) > 1:
            sortedperiods = sorted(periods)
            params_string = "from_%s_to_%s"%(sortedperiods[0],sortedperiods[-1])
        else:
            params_string = periods[0]
        # outpath is an optional label but used into the file path
        if outpath is None:
            outpath = self._get_user_filepath(dataset, 'gexf', "%s-graph"%params_string)
        else:
            outpath = self._get_user_filepath(dataset, 'gexf',  "%s_%s-graph"%(params_string, outpath) )
        outpath = abspath( outpath + ".gexf" )

        GEXFWriter = self._new_graph_writer(
            dataset,
            periods,
            "None",
            storage,
            generate=True,
            preprocess=False
        )

        periods_to_process = []
        ngram_index = set([])
        doc_index = set([])
        # checks periods and construct nodes' indices
        for period in periods:
            corpus = storage.loadCorpus( period )
            yield self.STATUS_RUNNING
            if corpus is not None:
                periods_to_process += [corpus]
                # union
                ngram_index |= set( corpus['edges']['NGram'].keys() )
                yield self.STATUS_RUNNING
                # union
                doc_index |= set( corpus['edges']['Document'].keys() )
            else:
                self.logger.debug('Period %s not found in database, skipping'%str(period))

        if len(ngram_index) == 0 or len(doc_index) == 0:
            yield self.STATUS_ERROR
            errmsg = "Graph not generated because : NGram index length = %d, Document index length = %d"%(len(ngram_index),len(doc_index))
            self.logger.warning(errmsg)
            raise RuntimeError(errmsg)
            return

        # updates default config with parameters
        update_ngramconfig = self.config['datamining']['NGramGraph']
        update_ngramconfig.update(ngramgraphconfig)
        update_documentconfig = self.config['datamining']['DocumentGraph']
        update_documentconfig.update(documentgraphconfig)

        # hack resolving the proximity parameter ambiguity
        #if update_ngramconfig['proximity']=='cooccurrences':
        #    ngram_matrix_reducer = graph.MatrixReducerFilter( ngram_index )
        #elif update_ngramconfig['proximity']=='pseudoInclusion':
        #    ngram_matrix_reducer = graph.PseudoInclusionMatrix( ngram_index )
        if update_ngramconfig['proximity']=='EquivalenceIndex':
            update_ngramconfig['nb_documents'] = len(doc_index)
        #    ngram_matrix_reducer = graph.EquivalenceIndexMatrix( ngram_index )
        #else:
        #    errmsg = "%s is not a valid NGram graph proximity"%update_ngramconfig['proximity']
        #    self.logger.error(errmsg)
        #    raise NotImplementedError(errmsg)
        #    return

        ngram_graph_class = _dynamic_get_class("tinasoft.pytextminer.graph.subgraph", "NgramGraph")
        ngram_matrix_class = _dynamic_get_class("tinasoft.pytextminer.graph.matrix", update_ngramconfig['proximity'])
        ngram_matrix_reducer = ngram_matrix_class(ngram_index)
        self.logger.debug("finished preparing params for generate_graph")
        # ngramgraph proximity is based on previously stored
        ngramsubgraph_gen = process_ngram_subgraph(
            self.config,
            dataset,
            periods_to_process,
            ngram_index,
            doc_index,
            ngram_matrix_reducer,
            update_ngramconfig,
            GEXFWriter,
            storage,
            ngram_graph_class
        )
        try:
            while 1:
                yield self.STATUS_RUNNING
                ngramsubgraph_gen.next()
        except StopIteration:
            self.logger.debug("finished NGramGraph")
            pass

        doc_graph_class = _dynamic_get_class("tinasoft.pytextminer.graph.subgraph", "DocGraph")

        doc_matrix_reducer = matrix.MatrixReducerFilter( doc_index )
        docsubgraph_gen = process_document_subgraph(
            self.config,
            dataset,
            periods_to_process,
            ngram_index,
            doc_index,
            doc_matrix_reducer,
            update_documentconfig,
            GEXFWriter,
            storage,
            doc_graph_class
        )
        try:
            while 1:
                yield self.STATUS_RUNNING
                docsubgraph_gen.next()
        except StopIteration:
            self.logger.debug("finished DocGraph")
            pass

        #if exportedges is True:
        self.logger.warning("exporting the full graph to %s"%outpath)
        GEXFWriter.graph['parameters']['data/source'] = "standalone"
        GEXFWriter.finalize(outpath, exportedges=True)
        # returns the absolute path of outpath
        #GEXFWriter.graph['parameters']['data/source'] = "browser"
        #GEXFWriter.finalize(outpath, exportedges=False)
        yield outpath
        return

    def index_archive(self,
            path,
            dataset,
            periods,
            whitelistpath,
            format,
            outpath=None,
            minCooc=1,
            store=None
        ):
        """
        Index a whitelist against an archive of articles,
        updates cooccurrences into storage,
        optionally exporting cooccurrence matrix
        """
        self._load_config()
        try:
            # path is an archive directory
            path = self._get_sourcefile_path(path)
        except IOError, ioe:
            self.logger.error(ioe)
            yield self.STATUS_ERROR
            return
        storage = self.get_storage(dataset, create=True, drop_tables=False)
        if storage == self.STATUS_ERROR:
            yield self.STATUS_ERROR
            return

        whitelist = self._import_whitelist(whitelistpath)
        # prepares extraction export path
        if outpath is not None:
            outpath = self._get_user_filepath(
                dataset,
                'cooccurrences',
                "%s-matrix.csv"%(whitelist['label'])
            )
            exporter = Writer("coocmatrix://"+outpath)
            whitelist_outpath = self._get_user_filepath(
                dataset,
                'cooccurrences',
                "%s-terms.csv"%(whitelist['label'])
            )
            whitelist_exporter = Writer("basecsv://"+whitelist_outpath)
        else:
            exporter = None

        archive = Reader( format + "://" + path, **self.config['datasets'] )
        archive_walker = archive.walkArchive(periods)
        try:
            period_gen, period = archive_walker.next()
            sc = indexer.ArchiveCounter(self.config['datasets'], storage)
            walkCorpusGen = sc.walk_period(whitelist, period_gen, period)
            try:
                while 1:
                    yield walkCorpusGen.next()
            except StopIteration:
                pass
            writeMatrixGen = sc.write_matrix(period, exporter, whitelist_exporter, minCooc)
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
            outpath=None,
            minCooc=1
        ):
        """
        returns a text file outpath containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        self._load_config()
        storage = self.get_storage(dataset, create=True, drop_tables=False)
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
        # is a generator
        return exporter.export_from_storage( storage, periods, minCooc )

    def _import_whitelist(
            self,
            whitelistpath,
            dataset = None,
            userstopwords = None,
            dialect="excel",
            encoding="utf_8"
        ):
        """
        import one or a list of whitelits files
        returns a whitelist object to be used as input of other methods
        """
        whitelist_id = self._get_filepath_id(whitelistpath)
        kwargs = {
            'dialect': dialect,
            'encoding': encoding
        }
        if whitelist_id is not None:
            ### whitelistpath EXISTS
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file()
        # NOT USED : TO CHECK
        elif isinstance(dataset, corpora.Corpora):
            self._load_config()
            storage = self.get_storage(dataset, create=False, drop_tables=False)
            if storage == self.STATUS_ERROR:
                return self.STATUS_ERROR
            # whitelistpath is a whitelist label into storage
            self.logger.debug("loading whitelist %s from storage"%whitelist_id)
            new_wl = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl.loadFromStorage(storage, dataset)
        elif exists(whitelistpath):
            ### whitelist path is a real path but not in a correct format
            whitelist_id = dataset
            self.logger.debug("loading whitelist from %s (%s)"%(whitelistpath, whitelist_id))
            wlimport = Reader('whitelist://'+whitelistpath, **kwargs)
            wlimport.whitelist = whitelist.Whitelist( whitelist_id, whitelist_id )
            new_wl = wlimport.parse_file()
        else:
            raise Exception("unable to find a whitelist at %s"%whitelistpath)
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
        self.logger.warning("loading user stopwords from %s"%path)
        return [stopwords.StopWordFilter( "file://%s" % path )]



class PytextminerApi(PytextminerFlowApi):
    def _eraseFlow(self, generator):
        """
        Flattens all API generator for simpler scripting of PytextminerApi
        """
        try:
            while 1:
                lastValue = generator.next()
                #print lastValue
        except StopIteration, si:
            return lastValue

    def extract_file(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).extract_file(*arg, **kwargs)
        return self._eraseFlow( generator )

    def index_file(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).index_file(*arg, **kwargs)
        return self._eraseFlow( generator )

    def graph_preprocess(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).graph_preprocess(*arg, **kwargs)
        return self._eraseFlow( generator )

    def generate_graph(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).generate_graph(*arg, **kwargs)
        return self._eraseFlow( generator )

    def index_archive(self, *arg, **kwargs):
        generator = super(PytextminerApi, self).index_archive(*arg, **kwargs)
        return self._eraseFlow( generator )


def _dynamic_get_class(mod_name, class_name):
    """returns a class given its name"""
    mod = __import__(mod_name, globals(), locals(), [class_name])
    return getattr(mod, class_name)
