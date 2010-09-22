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

import tinasoft

import datetime
import itertools
import math
from decimal import *

import logging
_logger = logging.getLogger('TinaAppLogger')

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
        'float64' : 'float',
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

    def addEdge( self, source, target, weight, type, overwrite, **kwargs ):
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

    def delEdge( self, source, target=None ):
        if target is None:
            del self.gexf['edges'][source]
        else:
            del self.gexf['edges'][source][target]


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
            self.edgethreshold[1] = float('inf')

        if 'nodethreshold' not in opts:
            self.nodethreshold = defaults['nodethreshold']
        else:
            self.nodethreshold = opts['nodethreshold']

        # max == 'inf' or max == 0 : set to float('inf')
        self.nodethreshold = [ float(self.nodethreshold[0]), float(self.nodethreshold[1]) ]
        if self.nodethreshold[1] == 0:
            self.nodethreshold[1] = float('inf')
        # overwrite this with the node type you want
        self.id_prefix = "SubGraph::"
        # get its own thread pool
        #self.thread_pool = threadpool.ThreadPool(50)

    def notify( self, count, name="" ):
        """
        notifer filtering quantity of messages
        """
        if count % 10000 == 0:
            _logger.debug( "%d %s subgraph %s processed"%(count,self.id_prefix,name) )

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

    def addEdge( self, graph, source, target, weight, type, overwrite, **kwargs ):
        """
        Adds an edge to the graph
        Source and Target are already graph nodes ID : eg "Subgraph::123457654"
        """
        graph.addEdge( self.id_prefix + source, self.id_prefix + target, weight, type, overwrite, **kwargs )

    def delEdge( self, graph, source, target=None ):
        if target is None:
            graph.delEdge( self.id_prefix + source, None )
        else:
            graph.delEdge( self.id_prefix + source, self.id_prefix + target )

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

    def __init__(self, db_load_obj, opts, defaults, whitelist):

        SubGraph.__init__(self, db_load_obj, opts, defaults)

        self.id_prefix = "NGram::"
        self.whitelist = whitelist

    def load( self, matrix, graph, corp ):
        for ngram, occ in self.whitelist['edges']['NGram'].iteritems():
            if occ <= self.nodethreshold[1] and occ >= self.nodethreshold[0]:
                self.addNode( graph, ngram, occ )
        for (ng1, ng2) in itertools.permutations(self.whitelist['edges']['NGram'].keys(), 2):
            # TODO update Documents in DB
            # LAST ERROR : matrix index out of range !!
            weight = matrix.get(ng1, ng2)
            if weight <= self.edgethreshold[1] and weight >= self.edgethreshold[0]:
                self.addEdge( graph, ng1, ng2, weight, 'directed', False )


class DocumentGraph(SubGraph):
    """
    A document graph constructor
    depends on Graph object

    defaults = {
        'edgethreshold': [0.0,2.0],
        'nodethreshold': [1,float('inf')]
    }
    """
    def __init__(self, db_load_obj, opts, defaults, whitelist):

        SubGraph.__init__(self, db_load_obj, opts, defaults)
        self.whitelist = whitelist
        self.id_prefix = "Document::"

    def load( self, matrix, graph, corp ):
        """
        Symmetric edges
        """
        # loads documents nodes
        for doc_id, occ in corp['edges']['Document'].iteritems():
            if occ <= self.nodethreshold[1] and occ >= self.nodethreshold[0]:
                self.addNode( graph, doc_id, occ )
        # loads documents edges
        for doc1, doc2 in itertools.permutations( corp['edges']['Document'].keys(), 2 ):
            # TODO update Documents in DB
            weight = matrix.get(doc1, doc2)
            if weight <= self.edgethreshold[1] and weight >= self.edgethreshold[0]:
                self.addEdge( graph, doc1, doc2, weight, 'undirected', False )

