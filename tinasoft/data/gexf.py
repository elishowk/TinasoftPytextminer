# -*- coding: utf-8 -*-
import tinasoft
from tinasoft import threadpool
from tinasoft.data import Handler

import datetime
import itertools
import math
from decimal import *


# Tenjin, the fastest template engine in the world !
import tenjin
from tenjin.helpers import *

# tinasoft logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class GEXFHandler(Handler):
    """
    A generic GEXF handler
    """
    options = {
        'locale'     : 'en_US.UTF-8',
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
        'bool' : 'boolean',
        'float' : 'float',
        'str' : 'string',
        'unicode' : 'string',
    }

    def __init__( self ):
        self.gexf = {
            'description' : "tinasoft",
            'creators'    : ["tinasoft team"],
            'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
            'type'        : 'static',
            'attrnodes'   : {},
            'attredges'   : {},
            'nodes': {},
            'edges' : {},
        }

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

    def addEdge( self, source, target, weight, type, overwrite=False, **kwargs ):
        if source not in self.gexf['edges']:
            self.gexf['edges'][source] = {}
        if target not in self.gexf['edges'][source]:
            self.gexf['edges'][source][target] = kwargs
            self.gexf['edges'][source][target]['weight'] = weight
            self.gexf['edges'][source][target]['type'] = type
            self.updateAttrEdges( kwargs )
        else:
            if overwrite is False:
                # sums the weight attr
                self.gexf['edges'][source][target]['weight'] += weight
            else:
                # overwrites weight
                self.gexf['edges'][source][target]['weight'] = weight


class SubGraph():
    """
    Base class for subgraph classes, must be under classed
    """
    def __init__(self, db_load_object, opts, defaults):
        """
        initialize the subgraph instance
        @db_load_object must must the database method returning the type of object
        @opts is a dict of paramaters
        """
        _logger.debug(opts)
        self.db_load_object = db_load_object
        # temp store for database objects
        self.cache = {}
        # config params OR defaults
        if 'edgethreshold' not in opts:
            self.edgethreshold = defaults['edgethreshold']
        else:
            self.edgethreshold = opts['edgethreshold']

        # max == 'inf' or max == 0 : set to float('inf')
        self.edgethreshold = [ float(self.edgethreshold[0]), float(self.edgethreshold[1]) ]
        if self.edgethreshold[1] == 0:
            _logger.debug("transformed 0 to inf")
            self.edgethreshold[1] = float('inf')

        if 'nodethreshold' not in opts:
            self.nodethreshold = defaults['nodethreshold']
        else:
            self.nodethreshold = opts['nodethreshold']

        # max == 'inf' or max == 0 : set to float('inf')
        self.nodethreshold = [ float(self.nodethreshold[0]), float(self.nodethreshold[1]) ]
        if self.nodethreshold[1] == 0:
            _logger.debug("transformed 0 to inf")
            self.nodethreshold[1] = float('inf')
        try:
            if 'proximity' in opts:
                # string eval to method
                self.proximity = eval(opts['proximity'])
            elif 'proximity' in defaults:
                self.proximity = eval(defaults['proximity'])
            else:
                self.proximity = SubGraph.proximity
        except Exception, exc:
            _logger.warning("unable to load proximity measure method, switching to default")
            _logger.warning(repr(exc))
            self.proximity = SubGraph.proximity
        # overwrite this with the node type you want
        self.id_prefix = "SubGraph::"
        # get its own threrad pool
        #self.thread_pool = threadpool.ThreadPool(50)


    @staticmethod
    def proximity( node1_weight, node2_weight, edge_weight, graph, alpha=None ):
        """
        must be overwritten or replaced by another staticmethod with same params
        """
        return edge_weight

    def notify( self, count ):
        """
        notifer filtering quantity of messages
        """
        if count % 10000 == 0:
            _logger.debug( "%d %s subgraph's edges processed"%(count,self.id_prefix) )

    def mapEdges( self, graph ):
        """
        Method to overwrite
        Maps the whole graph to transform, filter OR create edges weight to the result of SubGraph.proximity measure
        """
        pass

    def mapNodes(self, graph):
        """
        Method to overwrite
        Maps the whole graph to transform or filter nodes
        """
        pass

    def addEdge( self, graph, source, target, weight, type, overwrite=False, **kwargs ):
        """
        Adds an edge to the graph
        Source and Target are already graph nodes ID : eg "Subgraph::123457654"
        """
        graph.addEdge( self.id_prefix + source, self.id_prefix + target, weight, type, overwrite, **kwargs )

    def addNode( self, graph, obj_id, weight ):
        """
        Adds a node to the graph
        """
        graphnodeid = self.id_prefix+obj_id
        if graphnodeid not in self.cache:
            obj = self.db_load_object( obj_id )
            if obj is None:
                _logger.error( "Node object %s not found in db"%obj_id )
                return
            self.cache[ graphnodeid ] = obj
        nodeattr = {
            #'category' : 'NGram',
            'label' :  self.cache[ graphnodeid ].getLabel(),
            #'id' :  self.cache[ nodeid ]['id'],
        }
        graph.addNode( graphnodeid, weight, **nodeattr )



class NGramGraph(SubGraph):
    """
    A NGram graph constructor
    depends on Graph object

    default_config = {
        'edgethreshold': [0.0,1.0],
        'nodethreshold': [1,float('inf')],
        'alpha': 0.1
    }
    """

    def __init__(self, db_load_obj, opts, defaults):
        SubGraph.__init__(self, db_load_obj, opts, defaults)
        if 'alpha' not in opts:
            self.alpha = self.defaults['alpha']
        else:
            self.alpha = opts['alpha']

        self.id_prefix = "NGram::"


    @staticmethod
    def pseudoInclusionProx( occ1, occ2, cooc, graph, alpha ):
        try:
            prox = (( float(cooc) / float(occ1) )**alpha) * (( float(cooc) / float(occ2) )**(float(1)/float(alpha)))
            return prox
        except Exception, e:
            _logger.error(e)
            _logger.error( "Parameters occ1=%d, occ2=%d, cooc=%d, alpha=%d"%(occ1, occ2, cooc, alpha) )
            return 0

    def mapEdges( self, graph ):
        """
        Maps the whole graph to TRANSFORM raw cooccurrences (edge weight)
        to the result of a given proximity measure
        edge weight MUST be already computed here, to be transformed by promity()
        mapNodes always before madEdges
        """
        _logger.debug( "mapping ngrams edges")
        count = 0
        # walk all edges
        for source in graph.gexf['edges'].keys():
            (sourceCategory, sourceDbId) = source.split('::')
            # remove edges of unexistant node
            if source not in graph.gexf['nodes']:
                del graph.gexf['edges'][source]
                continue
            elif sourceCategory == 'NGram':
                for target in graph.gexf['edges'][source].keys():
                    (targetCategory, targetDbId) = target.split('::')
                    # remove edges
                    if target == source or target not in graph.gexf['nodes']:
                        del graph.gexf['edges'][source][target]
                        continue
                    elif targetCategory == 'NGram':
                        # computes the proximity
                        occ1 = graph.gexf['nodes'][source]['weight']
                        occ2 = graph.gexf['nodes'][target]['weight']
                        cooc = graph.gexf['edges'][source][target]['weight']
                        # TODO push in thread pool
                        #prox = self.thread_pool.queueTask( self.proximity, args=( occ1, occ2, cooc, self.alpha, graph ) )
                        prox = self.proximity( occ1, occ2, cooc, graph, self.alpha )
                        count+=1
                        self.notify(count)
                        if prox <= self.edgethreshold[1] and prox >= self.edgethreshold[0]:
                            self.addEdge(graph, sourceDbId, targetDbId, prox, 'directed', True)
                            #graph.gexf['edges'][source][target]['weight'] = prox
                            #graph.gexf['edges'][source][target]['type'] = 'directed'
                        else:
                            del graph.gexf['edges'][source][target]


    def mapNodes(self, graph):
        """
        Filters NGramGraph nodes
        """
        _logger.debug( "mapping ngrams nodes")
        for source in graph.gexf['nodes'].keys():
            sourceCategory = source.split('::')[0]
            if sourceCategory == 'NGram':
                if graph.gexf['nodes'][source]['weight'] > self.nodethreshold[1]\
                    or graph.gexf['nodes'][source]['weight'] < self.edgethreshold[0]:
                        del graph.gexf['nodes'][source]
                        if source in self.cache:
                            del self.cache[source]


class DocumentGraph(SubGraph):
    """
    A document graph constructor
    depends on Graph object

    defaults = {
        'edgethreshold': [0.0,2.0],
        'nodethreshold': [1,float('inf')]
    }
    """
    def __init__(self, db_load_obj, opts, defaults, whitelist=None):

        SubGraph.__init__(self, db_load_obj, opts, defaults)
        self.whitelist = whitelist
        self.id_prefix = "Document::"

    @staticmethod
    def sharedNGrams( doc1, doc2, whitelist, graph ):
        """
        intersection of doc1 ngrams with a whitelist
        then return length of the intersection with doc2 ngrams
        """
        if whitelist is not None:
            doc1ngrams = set( doc1['edges']['NGram'].keys() ) & set( whitelist['edges']['NGram'] )
        else:
            doc1ngrams = set( doc1['edges']['NGram'].keys() )
        return len( doc1ngrams & set( doc2['edges']['NGram'].keys() ) )

    @staticmethod
    def logJacquard( doc1, doc2, whitelist, graph ):
        """
        Jacquard-like distance
        """
        if whitelist is not None:
            doc1ngrams = set( doc1['edges']['NGram'].keys() ) & set( whitelist['edges']['NGram'] )
            doc2ngrams = set( doc2['edges']['NGram'].keys() ) & set( whitelist['edges']['NGram'] )
        else:
            doc1ngrams = set( doc1['edges']['NGram'].keys() )
            doc2ngrams = set( doc2['edges']['NGram'].keys() )
        ngramsintersection = doc1ngrams & doc2ngrams
        ngramsunion = doc1ngrams | doc2ngrams
        weight = 0
        if len(ngramsunion) > 0:
            weight = sum(
                [ float(1)/float( math.log( 1+ graph.gexf['nodes']['NGram::'+ngramid]['weight'] )) for ngramid in ngramsintersection if 'NGram::'+ngramid in graph.gexf['nodes'] ],
                0
            ) / sum(
                [ float(1)/float( math.log( 1+ graph.gexf['nodes']['NGram::'+ngramid]['weight'] )) for ngramid in ngramsunion if 'NGram::'+ngramid in graph.gexf['nodes'] ],
                0
            )
        return weight

    def mapEdges( self, graph ):
        """
        Maps the whole graph to CREATE document edges and weights
        """
        _logger.debug( "mapping document edges")
        total=0
        for (id1, id2) in itertools.combinations(graph.gexf['nodes'].keys(), 2):
            id1Category = id1.split('::')[0]
            id2Category = id2.split('::')[0]
            if id1Category != 'Document' or id2Category != 'Document': continue
            if id1 == id2: continue
            total+=1
            self.notify(total)
            # TODO push to thread pool
            weight = self.proximity( self.cache[id1], self.cache[id2], self.whitelist, graph )
            # weight filtering
            if weight <= self.edgethreshold[1] and weight >= self.edgethreshold[0]:
                self.addEdge( graph, self.cache[id1]['id'], self.cache[id2]['id'], weight, 'mutual', overwrite=False )

    def mapNodes(self, graph):
        """
        Filters DocumentGraph nodes
        """
        _logger.debug( "mapping document nodes")
        for source in graph.gexf['nodes'].keys():
            sourceCategory = source.split('::')[0]
            if source in graph.gexf['nodes'] and sourceCategory == 'NGram':
                if graph.gexf['nodes'][source]['weight'] > self.nodethreshold[1]\
                    or graph.gexf['nodes'][source]['weight'] < self.edgethreshold[0]:
                        del graph.gexf['nodes'][source]
                        if source in self.cache:
                            del self.cache[source]


class Exporter (GEXFHandler):
    """
    A Gexf Exporter engine providing various graph construction methods
    """

    def notify(self, count):
        if count % 100 == 0:
            _logger.debug( "%d graph nodes processed"%count )

    def ngramDocGraph(self, path, db, periods, meta={}, whitelist=None, ngramgraphconfig=None, documentgraphconfig=None):
        """
        uses Cooc from database to write a NGram & Document graph for a given list of periods
        using Cooccurencesfor NGram proximity computing
        """
        if len(periods) == 0: return
        count = 0
        # init graph object
        graph = Graph()
        graph.gexf.update(meta)
        # init subgraphs objects
        ngramGraph = NGramGraph( db.loadNGram, ngramgraphconfig, self.NGramGraph )
        docGraph = DocumentGraph( db.loadDocument, documentgraphconfig, self.DocumentGraph, whitelist=whitelist)
        # TODO Create an internal cooccurrences package to move matrix loading
        #coocMatrix = cooccurrences.CoocMatrix( len( whitelist.keys() ) )
        for period in periods:
            # loads the corpus (=period) object
            corp = db.loadCorpus(period)
            if corp is None:
                _logger.warning("Period %s not found, passing"%period)
                continue
            # TODO updates the cooc matrix
            #coocReader = cooccurrences.MapReduce( db, whitelist, period )
            #coocReader.readMatrix()
            #coocMatrix += coocReader.matrix
            # add documents nodes
            for doc_id, occ in corp['edges']['Document'].iteritems():
                docGraph.addNode( graph, doc_id, occ )
                count += 1
                self.notify(count)
            # gets the database Cooc matrix lines cursor for the current period
            coocmatrixlines = db.selectCorpusCooc(period)
            try:
                while 1:
                    ngid1,row = coocmatrixlines.next()
                    # whitelist checking
                    if whitelist is not None and whitelist.test(ngid1) is False:
                        _logger.error("skipping ngram node : not in the whitelist")
                        continue
                    # integrity checking
                    if ngid1 not in corp['edges']['NGram']:
                        _logger.error("skipping ngram node : not in the corpus")
                        continue
                    occ1 = corp['edges']['NGram'][ngid1]
                    # source NGram node
                    ngramGraph.addNode( graph, ngid1, occ1 )
                    # OBSOLETE : doc-ngram edges
                    #for docid, dococcs in ngramGraph.cache["NGram::"+ngid1]['edges']['Document'].iteritems():
                    #    if "Document::"+docid in docGraph.cache:
                    #        graph.addEdge( "Document::"+docid, "NGram::"+ngid1, dococcs, 'mutual' )
                    # adds target NGram nodes
                    for ngid2, cooc in row.iteritems():
                        # whitelist check
                        if whitelist is not None and whitelist.test(ngid2) is False:
                            continue
                        # weight must be 0 here because the coocmatrix is symmetric
                        ngramGraph.addNode( graph, ngid2, 0 )
                        # cooccurrences is the temporary edge's weight
                        ngramGraph.addEdge( graph, ngid1, ngid2, cooc, 'directed', overwrite=False )

                    self.notify(count)
                    count += 1

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
        # empty db object cache
        ngramGraph.cache = {}
        docGraph.cache = {}
        # compiles then writes the gexf file
        open(path, 'w+b').write(self.render( graph.gexf ))
        # return relative path
        return path
