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

#from tinasoft import threadpool
from tinasoft.data import Handler
from tinasoft.pytextminer import graph, PyTextMiner, ngram, document


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

class Exporter(GEXFHandler):
    """
    A Gexf Exporter engine providing various graph construction methods
    """

    def notify(self, count):
        if count % 100 == 0:
            _logger.debug( "%d graph nodes processed"%count )


class Exporter(Exporter):
    """
    A Gexf Exporter engine providing various graph construction methods
    """
    def new_graph(
            self,
            path,
            storage,
            meta
        ):
        self.path = path
        self.storage = storage
        # init graph object
        self.graph = {
            'nodes' : {},
            'storage': storage,
        }
        self.graph.update(meta)

    def notify(self, count):
        if count % 200 == 0:
            _logger.debug( "%d graph nodes processed"%count )

    def load_subgraph(self, category, matrix, subgraphconfig=None):
        """
        uses a numpy array to add a Document subgraph to the global graph object
        """
        self.graph['nodes'][category] = {}
        nodecount = 0
        _logger.debug("loading %s edges into storage/gexf"%category)
        rows = matrix.extract_matrix( subgraphconfig )
        try:
            while 1:
                id, row = rows.next()
                obj = self.storage.load( id, category )
                if obj is None:
                    continue
                weight = matrix.get(id, id)
                self.graph['nodes'][category][id] = matrix.get(id, id)
                edges = { category : row }
                self.storage.insert( PyTextMiner.updateEdges( edges, obj ), category )
                nodecount += 1
                self.notify(nodecount)
        except StopIteration, si:
            pass

    def finalize(self, exportedges=False):
        """
        final method compiling the graph and writing it to file
        """
        # arguments passed to tenjin, to optionally write edges into the gexf
        self.graph['exportedges'] = exportedges
        open(self.path, 'w+b').write(self.render( self.graph ))
        # returns relative path
        return self.path
