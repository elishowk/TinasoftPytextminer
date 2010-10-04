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
import numpy

import logging
_logger = logging.getLogger('TinaAppLogger')

class Graph(dict):
    """
    Main Graph class for graph construction
    """

    def __init__( self, storage ):
        self.storage= storage
        self.nodes = {}

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
        self.id_prefix = "NGram::"

    def load( self, matrix, graph ):
        """
        loads nodes then edges into graph object
        """
        index = {}
        for key, i in matrix.reverseindex.iteritems():
            index[i] = key

        for ngram in matrix.reverseindex.keys():
            occ = matrix.get(ngram, ngram)
            if occ <= self.nodethreshold[1] and occ >= self.nodethreshold[0]:
                self.addNode( graph, ngram, occ )
        i = 0
        while matrix.array.shape[0] > 0:
            row = matrix.array[0,:]
            matrix.array = numpy.delete( matrix.array, 0, 0 )
            ng1 = index[i]
            for j in range(len(row)):
                if i != j:
                    ng2 = index[j]
                    weight = row[j]
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
    def __init__(self, db_load_obj, opts, defaults):
        SubGraph.__init__(self, db_load_obj, opts, defaults)
        self.id_prefix = "Document::"
        #self.periods = periods

    def load( self, matrix, graph ):
        """
        Symmetric edges
        """
        # loads document nodes
        for doc_id in matrix.reverseindex.keys():
            occ = matrix.get(doc_id, doc_id)
            if occ <= self.nodethreshold[1] and occ >= self.nodethreshold[0]:
                self.addNode( graph, doc_id, occ )
        # only combinations, matrix is symmetric
        for (doc1, doc2) in itertools.combinations( matrix.reverseindex.keys(), 2 ):
            # TODO update Documents in DB
            weight = matrix.get(doc1, doc2)
            if weight <= self.edgethreshold[1] and weight >= self.edgethreshold[0]:
                self.addEdge( graph, doc1, doc2, weight, 'undirected', False )

