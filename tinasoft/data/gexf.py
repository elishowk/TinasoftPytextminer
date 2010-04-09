# -*- coding: utf-8 -*-
import tinasoft
from tinasoft.data import Exporter
from tinasoft.pytextminer import cooccurrences
import datetime
import itertools
import math

# Tenjin, the fastest template engine in the world !
import tenjin
from tenjin.helpers import *
''
# tinasoft logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class GEXFHandler(Exporter):
    """
    A generic GEXF handler
    """
    options = {
        'locale'     : 'en_US.UTF-8',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None,
        'template'   : 'shared/gexf/gexf.template',
    }

    def __init__(self, path, **opts):
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')
        self.engine = tenjin.Engine()

    def render( self, gexf ):
        return self.engine.render(self.template, gexf)

class Graph():
    """
    Main Graph class for graph construction
    """
    attrTypes = {
        'int' : 'integer',
        'int8' : 'integer',
        'int16' : 'integer',
        'int32' : 'integer',
        'long' : 'long',
        'bool': 'boolean',
        'float' : 'float',
        'str' : 'string',
        'unicode' : 'string',
    }

    def __init__( self ):
        self.gexf = {
            'description' : "tinasoft graph",
            'creators'    : [],
            'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
            'type'        : 'static',
            'attrnodes'   : {},
            'attredges'   : {},
            'nodes': {},
            'edges' : {},
        }

    #def getWeight( self, method, *args ):
    #    return method( *args )

    def updateAttrNodes( self, attr ):
        for name, value in attr.iteritems():
            if name not in self.gexf['attrnodes']:
                self.gexf['attrnodes'][name] = self.attrTypes[ value.__class__.__name__ ]

    def updateAttrEdges( self, attr ):
        for name, value in attr.iteritems():
            if name not in self.gexf['attredges']:
                self.gexf['attredges'][name] = self.attrTypes[ value.__class__.__name__ ]

    def addNode( self, nodeid, weight, **kwargs ):
        if nodeid not in self.gexf['nodes']:
            self.gexf['nodes'][nodeid] = kwargs
            self.gexf['nodes'][nodeid]['weight'] = weight
            self.updateAttrNodes( kwargs )
        else:
            # sums the weight attr
            self.gexf['nodes'][nodeid]['weight'] += weight

    def addEdge( self, source, target, weight, type, **kwargs ):
        if source not in self.gexf['edges']:
            self.gexf['edges'][source] = {}
        if target not in self.gexf['edges'][source]:
            self.gexf['edges'][source][target] = kwargs
            self.gexf['edges'][source][target]['weight'] = weight
            self.gexf['edges'][source][target]['type'] = type
            self.updateAttrEdges( kwargs )
        else:
            # sums the weight attr
            self.gexf['edges'][source][target]['weight'] += weight

class SubGraph():
    """
    Base class for subgraph classes
    """
    def __init__(self, db, opts):

        self.db = db
        self.cache = {}

        if 'edgethreshold' not in opts:
            self.edgethreshold = self.defaults['edgethreshold']
        else:
            if opts['edgethreshold'][1] == 'inf':
                opts['edgethreshold'][1] = float('inf')
            self.edgethreshold = opts['edgethreshold']

        if 'nodethreshold' not in opts:
            self.nodethreshold = self.defaults['nodethreshold']
        else:
            if opts['nodethreshold'][1] == 'inf':
                opts['nodethreshold'][1] = float('inf')
            self.nodethreshold = opts['nodethreshold']


class NGramGraph(SubGraph):
    """
    A NGram graph constructor
    depends on Graph object
    """

    defaults = {
        'edgethreshold': [0.0,1.0],
        'nodethreshold': [1,float('inf')]
    }

    def __init__(self, db, opts):
        SubGraph.__init__(self,db, opts)

        if 'proximity' not in opts:
            self.proximity = NGramGraph.genSpecProx
        else:
            self.proximity = opts['proximity']

        if 'alpha' not in opts:
            self.alpha = 0.01
        else:
            self.alpha = opts['alpha']

    @staticmethod
    def genSpecProx( occ1, occ2, cooc, alpha ):
        if 0 in [ occ1, occ2 ]:
            _logger.error( "Zero occ found" )
            return 0
        prox = (( float(cooc) / float(occ1) )**alpha) * (float(cooc) / float(occ2))
        return prox

    def notify( self, count ):
        if count % 500 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d ngram's edges processed"%count
            )
            _logger.debug( "%d ngram's edges processed"%count )

    def mapEdges( self, graph ):
        """
        Maps the whole graph to transform cooc edge weight to gen-spec prox
        """
        count = 0
        for source in graph.gexf['edges'].keys():
            sourceCategory = source.split('::')[0]
            if source in graph.gexf['nodes'] and sourceCategory == 'NGram':
                for target in graph.gexf['edges'][source].keys():
                    targetCategory = target.split('::')[0]
                    if target == source:
                        del graph.gexf['edges'][source][target]
                        continue
                    if target in graph.gexf['nodes'] and targetCategory == 'NGram':
                        occ1 = graph.gexf['nodes'][source]['weight']
                        occ2 = graph.gexf['nodes'][target]['weight']
                        cooc = graph.gexf['edges'][source][target]['weight']

                        prox = self.proximity( occ1, occ2, cooc, self.alpha )
                        count+=1
                        self.notify(count)
                        if prox <= self.edgethreshold[1] and prox >= self.edgethreshold[0]:
                            graph.gexf['edges'][source][target]['weight'] = prox
                            graph.gexf['edges'][source][target]['type'] = 'directed'
                        else:
                            del graph.gexf['edges'][source][target]

    def mapNodes(self, graph):
        for source in graph.gexf['nodes'].keys():
            sourceCategory = source.split('::')[0]
            if source in graph.gexf['nodes'] and sourceCategory == 'NGram':
                if graph.gexf['nodes'][source]['weight'] > self.nodethreshold[1]\
                    or graph.gexf['nodes'][source]['weight'] < self.edgethreshold[0]:
                        del graph.gexf['nodes'][source]
                        del self.cache[source.split('::')[1]]

    def addEdge( self, graph,  source, target, weight, type, **kwargs ):
        #kwargs['cooccurrences'] = weight
        graph.addEdge( 'NGram::'+source, 'NGram::'+target, weight, type, **kwargs )

    def addNode( self, graph, ngram_id, weight ):
        nodeid = 'NGram::'+ngram_id
        if nodeid not in self.cache:
            ngobj = self.db.loadNGram( ngram_id )
            self.cache[ nodeid ] = ngobj
        nodeattr = {
            #'category' : 'NGram',
            'label' :  self.cache[ nodeid ]['label'],
            #'id' :  self.cache[ nodeid ]['id'],
        }
        graph.addNode( nodeid, weight, **nodeattr )


class DocumentGraph(SubGraph):
    """
    A document graph constructor
    depends on Graph object
    """

    defaults = {
        'edgethreshold': [0.0,2.0],
        'nodethreshold': [1,float('inf')]
    }

    def __init__(self, db, opts, whitelist = None):

        SubGraph.__init__(self,db, opts)

        if 'proximity' not in opts:
            self.proximity = DocumentGraph.inverseOccNGramsEdgeWeight
        else:
            self.proximity = opts['proximity']

        self.whitelist = whitelist


    @staticmethod
    def sharedNGramsEdgeWeight( doc1, doc2, whitelist ):
        """
        intersection of doc1 ngrams with a whitelist
        then return length of the intersection with doc2 ngrams
        """
        if whitelist is not None:
            doc1ngrams = set( doc1['edges']['NGram'].keys() ) & set( whitelist )
        else:
            doc1ngrams = set( doc1['edges']['NGram'].keys() )
        return len( doc1ngrams & set( doc2['edges']['NGram'].keys() ) )

    @staticmethod
    def inverseOccNGramsEdgeWeight( doc1, doc2, whitelist, graph ):
        """
        intersection of doc1 & doc2 ngrams with a whitelist
        then returns :
            - 0 if no common ngrams found
            - a very little floating value if intersection's ngrams have many occurrences
            - ~1.44 if intersection's ngrams occurs only 1 time
        """
        if whitelist is not None:
            doc1ngrams = set( doc1['edges']['NGram'].keys() ) & set( whitelist )
        else:
            doc1ngrams = set( doc1['edges']['NGram'].keys() )
        ngrams = doc1ngrams & set( doc2['edges']['NGram'].keys() )
        return sum(
            [(1/( math.log( 1+ graph.gexf['nodes']['NGram::'+ngramid]['weight'] ) )) for ngramid in ngrams],
            0
        )

    def notify( self, count ):
        if count % 1000 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d document's edges processed"%count
            )
            _logger.debug( "%d document's edges processed"%count )

    def mapEdges( self, graph ):
        _logger.debug( "Documents in cache = %d"%len(self.cache.keys()) )
        total=0
        for (docid1, docid2) in itertools.combinations(self.cache.keys(), 2):
            if docid1 == docid2: continue
            total+=1
            self.notify(total)
            weight = self.proximity( self.cache[docid1], self.cache[docid2], self.whitelist, graph )
            if weight <= self.edgethreshold[1] and weight >= self.edgethreshold[0]:
                self.addEdge( graph, self.cache[docid1]['id'], self.cache[docid2]['id'], weight, 'mutual' )

    def mapNodes(self, graph):
        for source in graph.gexf['nodes'].keys():
            sourceCategory = source.split('::')[0]
            if source in graph.gexf['nodes'] and sourceCategory == 'NGram':
                if graph.gexf['nodes'][source]['weight'] > self.nodethreshold[1]\
                    or graph.gexf['nodes'][source]['weight'] < self.edgethreshold[0]:
                        del graph.gexf['nodes'][source]
                        del self.cache[source.split('::')[1]]

    def addEdge( self, graph, source, target, weight, type, **kwargs ):
        if weight > 0:
            graph.addEdge( 'Document::'+source, 'Document::'+target, weight, type, **kwargs )

    def addNode( self, graph,  doc_id, weight ):
        nodeid = 'Document::'+doc_id
        if nodeid not in self.cache:
            docobj = self.db.loadDocument( doc_id )
            if docobj is None:
                _logger.error( "Document %s not found in db"%doc_id )
                return
            self.cache[ nodeid ] = docobj
        nodeattr = {
            'label' : self.cache[ nodeid ]['label'],
        }
        graph.addNode( nodeid, weight, **nodeattr )


class Exporter (GEXFHandler):
    """
    A Gexf Exporter engine providing various graph construction methods
    """

    def notify( self ):
        if self.count % 100 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d graph nodes processed"%self.count
            )
            _logger.debug( "%d graph nodes processed"%self.count )

    def ngramDocGraph(self, db, periods, meta={}, whitelist=None):
        """
        uses Cooc from database to write a cooc-proximity based
        graph for a given list of periods
        """
        if len(periods) == 0: return
        self.count = 1
        graph = Graph()
        graph.gexf.update(meta)
        ngramGraph = NGramGraph( db, self.NGramGraph )
        docGraph = DocumentGraph( db, self.DocumentGraph, whitelist=whitelist)
        # TODO move patrix transformation into the cooc object
        #coocMatrix = cooccurrences.CoocMatrix( len( whitelist.keys() ) )
        for period in periods:
            # loads the corpus (=period) object
            corp = db.loadCorpus(period)
            # updates the cooc matrix
            #coocReader = cooccurrences.MapReduce( db, whitelist, period )
            #coocReader.readMatrix()
            #coocMatrix += coocReader.matrix
            # add documents nodes
            for doc_id, occ in corp['edges']['Document'].iteritems():
                docGraph.addNode( graph, doc_id, occ )
                self.count += 1
                self.notify()
            # gets the database cursor for the current period
            coocmatrix = db.selectCorpusCooc(period)
            try:
                while 1:
                    ngid1,row = coocmatrix.next()
                    # whitelist check
                    if whitelist is not None and ngid1 not in whitelist:
                        continue
                    if ngid1 not in corp['edges']['NGram']:
                        continue
                    occ1 = corp['edges']['NGram'][ngid1]
                    # source NGram node
                    ngramGraph.addNode( graph, ngid1, occ1 )
                    # doc-ngram edges
                    #for docid, dococcs in ngramGraph.cache["NGram::"+ngid1]['edges']['Document'].iteritems():
                    #    if "Document::"+docid in docGraph.cache:
                    #        graph.addEdge( "Document::"+docid, "NGram::"+ngid1, dococcs, 'mutual' )
                    # target NGram nodes
                    for ngid2, cooc in row.iteritems():
                        # whitelist check
                        if whitelist is not None and ngid2 not in whitelist:
                            continue
                        # weight must be 0 here because the coocmatrix is symmetric
                        ngramGraph.addNode( graph, ngid2, 0)
                        # cooccurrences is the temporary edge's weight
                        ngramGraph.addEdge( graph, ngid1, ngid2, cooc, 'directed' )

                    self.notify()
                    self.count += 1

            # End of database cursor
            except StopIteration:
                pass
            # global exception handler
            except Exception, e:
                import traceback
                _logger.error( traceback.format_exc() )
                return tinasoft.TinaApp.STATUS_ERROR
        ngramGraph.mapNodes( graph )
        ngramGraph.mapEdges( graph )
        docGraph.mapNodes( graph )
        docGraph.mapEdges( graph )
        ngramGraph.cache = {}
        docGraph.cache = {}
        #_logger.debug( graph.gexf )
        return self.render( graph.gexf )
