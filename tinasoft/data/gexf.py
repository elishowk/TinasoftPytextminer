# -*- coding: utf-8 -*-
import tinasoft
from tinasoft.data import Exporter
from tinasoft.pytextminer import cooccurrences
import datetime
import itertools

# Tenjin, the fastest template engine in the world !
import tenjin
from tenjin.helpers import *
''
# tinasoft logger
import logging
_logger = logging.getLogger('TinaAppLogger')

# generic GEXF handler
class GEXFHandler(Exporter):

    options = {
        'locale'     : 'en_US.UTF-8',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None,
        'threshold'  : 2,
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
            self.gexf['edges'][source][target]['weight'] += weight


class NGramGraph():

    def __init__(self, db, threshold=[0,1], alpha=0.01 ):
        self.db = db
        self.threshold = threshold
        self.alpha = alpha
        self.cache = {}

    @staticmethod
    def genSpecProx( occ1, occ2, cooc, alpha ):
        prox = (( float(cooc) / float(occ1) )**alpha) * (float(cooc) / float(occ2))
        return prox

    def notify( self, count ):
        if count % 200 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d ngram's edges processed"%count
            )
            _logger.debug( "%d ngram's edges processed"%count )

    def mapEdges( self, graph ):
        """
        Maps the whole graph to transform cooc edge weight to gen-spec edge weight
        """
        count = 0
        for source in graph.gexf['edges'].keys():
            if source in graph.gexf['nodes'] and graph.gexf['nodes'][source]['category'] == 'NGram':
                for target in graph.gexf['edges'][source].keys():
                    if target in graph.gexf['nodes'] and graph.gexf['nodes'][target]['category'] == 'NGram':
                        occ1 = graph.gexf['nodes'][source]['weight']
                        occ2 = graph.gexf['nodes'][target]['weight']
                        cooc = graph.gexf['edges'][source][target]['weight']
                        prox = NGramGraph.genSpecProx( occ1, occ2, cooc, self.alpha )
                        count+=1
                        self.notify(count)
                        if prox <= self.threshold[1] and prox >= self.threshold[0]:
                            graph.gexf['edges'][source][target]['weight'] = prox
                            graph.gexf['edges'][source][target]['type'] = 'directed'
                        else:
                            del graph.gexf['edges'][source][target]

    def addEdge( self, graph,  source, target, weight, type, **kwargs ):
        kwargs['cooccurrences'] = weight
        graph.addEdge( 'NGram::'+source, 'NGram::'+target, weight, type, **kwargs )

    def addNode( self, graph, ngram_id, weight ):
        nodeid = 'NGram::'+ngram_id
        if nodeid not in self.cache:
            ngobj = self.db.loadNGram( ngram_id )
            self.cache[ nodeid ] = ngobj
        nodeattr = {
            'category' : 'NGram',
            'label' :  self.cache[ nodeid ]['label'],
            'id' :  self.cache[ nodeid ]['id'],
        }
        graph.addNode( nodeid, weight, **nodeattr )


class DocumentGraph():

    def __init__(self, db):
        self.db = db
        self.cache = {}

    @staticmethod
    def sharedNGramsEdgeWeight( doc1, doc2 ):
        shared_ngrams = 0
        for ng1 in doc1['edges']['NGram'].iterkeys():
            for ng2 in doc2['edges']['NGram'].iterkeys():
                if ng1 == ng2:
                    shared_ngrams += 1
        return shared_ngrams

    def notify( self, count ):
        if count % 200 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d document's edges processed"%count
            )
            _logger.debug( "%d document's edges processed"%count )

    def mapEdges( self, graph ):
        _logger.debug( "Documents in cache = %d"%len(self.cache.keys()) )
        count=0
        for (docid1, docid2) in itertools.combinations( self.cache.keys(), 2):
            if docid1 == docid2: continue
            count+=1
            self.notify(count)
            weight = DocumentGraph.sharedNGramsEdgeWeight( self.cache[docid1], self.cache[docid2] )
            edgeattr = { 'shared_ngrams' : weight }
            self.addEdge( graph, self.cache[docid1]['id'], self.cache[docid2]['id'], weight, 'mutual',**edgeattr)

    def addEdge( self, graph, source, target, weight, type, **kwargs ):
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
            'category' : 'Document',
            'id' : doc_id,
            'label' : self.cache[ nodeid ]['label'],
            'occurrences' : weight,
            #'date': self.cache[ nodeid ]['date'],
            #'summary': '',
            #'keywords': docobj['doc_keywords'],
        }
        graph.addNode( nodeid, weight, **nodeattr )


class Exporter (GEXFHandler):
    """
    Gexf Engine
    """

    def notify( self ):
        if self.count % 25 == 0:
            tinasoft.TinaApp.notify( None,
                'tinasoft_runProcessCoocGraph_running_status',
                "%d graph nodes processed"%self.count
            )
            _logger.debug( "%d graph nodes processed"%self.count )

    def ngramDocGraph(self, db, periods, threshold=[0,1],\
        meta={}, whitelist=None, degreemax=None):
        """
        uses Cooc from database to write a cooc-proximity based
        graph for a given list of periods
        """
        if len(periods) == 0: return
        self.count = 1
        graph = Graph()
        graph.gexf.update(meta)
        ngramGraph = NGramGraph( db, threshold )
        docGraph = DocumentGraph( db )
        coocMatrix = cooccurrences.CoocMatrix( len( whitelist.keys() ) )
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
            generator = db.selectCorpusCooc(period)
            try:
                while 1:
                    ngid1,row = generator.next()
                    # whitelist check
                    if whitelist is not None and ngid1 not in whitelist:
                        continue
                    occ1 = corp['edges']['NGram'][ngid1]
                    #_logger.debug( "source node weight=%d"%occ1 )
                    # source NGram node
                    ngramGraph.addNode( graph, ngid1, occ1 )
                    # doc-ngram edges
                    for docid, dococcs in ngramGraph.cache["NGram::"+ngid1]['edges']['Document'].iteritems():
                        if "Document::"+docid in docGraph.cache:
                            graph.addEdge( "Document::"+docid, "NGram::"+ngid1, dococcs, 'mutual' )
                    # target NGram nodes
                    for ngid2, cooc in row.iteritems():
                        # whitelist check
                        if whitelist is not None and ngid2 not in whitelist:
                            continue
                        #occ2 = corp['edges']['NGram'][ngid2]
                        ngramGraph.addNode( graph, ngid2, 0)
                        # cooccurrences is the edge's weight
                        ngramGraph.addEdge( graph, ngid1, ngid2, cooc, 'directed' )

                    self.notify()
                    self.count += 1

            # End of database cursor
            except StopIteration:
                pass
                # TODO filters N nearest neighbours
            # global exception handler
            except Exception, e:
                import sys,traceback
                traceback.print_exc(file=sys.stdout)
                return tinasoft.TinaApp.STATUS_ERROR
        ngramGraph.mapEdges( graph )
        docGraph.mapEdges( graph )
        ngramGraph.cache = {}
        docGraph.cache = {}
        return self.render( graph.gexf )
