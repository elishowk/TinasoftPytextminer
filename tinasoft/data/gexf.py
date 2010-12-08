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
from tinasoft.pytextminer import PyTextMiner


# Tenjin, the fastest template engine in the world !
import tenjin
from tenjin.helpers import *

# Patch for Tenjin (Windows error 183)
def _write_binary_file(filename, content):
    f = None
    try:
        import random
        tmpfile = filename + str(random.random())[1:]
        f = open(tmpfile, 'w+b')
        f.write(content)
    finally:
         if f:
            f.close()
            import os
            try:
                os.rename(tmpfile, filename)
            except:
                os.remove(filename)
                os.rename(tmpfile, filename)

tenjin._write_binary_file = _write_binary_file

# tinasoft logger
import logging
_logger = logging.getLogger('TinaAppLogger')

NODE_COUNT_LOGGING = 1

class Exporter(Handler):
    """
    A Gexf Exporter engine providing multipartite graph exports
    """
    options = {
        'encoding'     : 'utf-8',
        'template'   : 'shared/gexf/gexf.default.template',
        'preprocess' : False,
        'generate' : True
    }

    def __init__(self, path, **opts):
        """
        ignoring path but loading options and tenjin Engine
        """
        self.loadOptions(opts)
        self.engine = tenjin.Engine()

    def new_graph(self, storage, meta, periods, generate, preprocess):
        """
        initialize the graph object and a storage connector
        """
        self.storage = storage
        self.graph = {
            'nodes' : {},
            'storage': storage,
            'attrnodes': {
                'weight': 'double'
            },
            'attredges': {},
        }
        self.graph.update(meta)
        self.periods = periods
        self.generate = generate
        self.preprocess = preprocess

    def notify(self, count):
        if count % NODE_COUNT_LOGGING == 0:
            _logger.debug( "%d graph nodes processed"%count )

    def load_subgraph(self, category, matrix, subgraphconfig):
        """
        uses a Graph.MatrixReducer type object
        to update the storage with new node edges
        """
        nodecount = 0
        _logger.debug("loading %s edges into graph"%category)
        rows = matrix.extract_matrix( subgraphconfig )
        try:
            while 1:
                nodeid, row = rows.next()
                if self.preprocess is True:
                    self._update_preprocess_db(nodeid, row, category)
                if self.generate is True:
                    self._update_graph_db(nodeid, row, category)
                    # adds node and its weight to the template var
                    self.graph['nodes'][category][nodeid] = matrix.get(nodeid, nodeid)
                nodecount += 1
                self.notify(nodecount)
                yield nodecount
        except StopIteration, si:
            if self.storage is not None:
                self.storage.flushQueues()
            return

    def _update_preprocess_db(self, nodeid, row, category):
        """
        updates the storage with a matrix row
        """
        if self.storage is None: return
        for period in self.periods:
            self.storage.updateGraphPreprocess(period, category, nodeid, row)    

    def _update_graph_db(self, nodeid, row, category):
        """
        updates the network in storage with a matrix row
        """
        if self.storage is None: return
        obj = self.storage.load(nodeid, category)
        if obj is None: return
        # temp edges row for the target category
        temp = { category: row }
        # overwrites the object
        self.storage.insert( PyTextMiner.updateEdges(temp, obj), category )

    def _render(self, gexf):
        """
        calls tenjin rendering method
        """
        return self.engine.render(self.template, gexf)

    def finalize(self, path, exportedges=False):
        """
        final method compiling the graph and writing it to file
        """
        # arguments passed to tenjin, to optionally write edges into the gexf
        self.graph['exportedges'] = exportedges
        open(path, 'w+b').write(self._render( self.graph ))
        # returns the same path
        return path
