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

from tinasoft.pytextminer import graph


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
            db,
            #periods,
            #whitelist,
            meta
        ):
        self.path = path
        self.db = db
        # init graph object
        self.graph = {
            'nodes' : {},
            'storage': db,
        }
        self.graph.update(meta)

    def notify(self, count):
        if count % 100 == 0:
            _logger.debug( "%d graph nodes processed"%count )

    def load_ngrams(self, ngrammatrix, ngramgraphconfig=None):
        """
        uses numpy array to write a NGram subgraph to the graph object
        """
        #TODO write and filter edges
        self.graph['nodes']['NGram'] = {}
        for key in ngrammatrix.reverseindex.keys():
            weight = ngrammatrix.get(key, key)
            if weight <= ngramgraphconfig['nodethreshold'][1] and weight >= ngramgraphconfig['nodethreshold'][0]:
                self.graph['nodes']['NGram'][key] = ngrammatrix.get(key, key)

    def load_documents(self, docmatrix, documentgraphconfig=None):
        """
        uses a numpy array to add a Document subgraph to the global graph object
        """
        #TODO write and filter edges
        self.graph['nodes']['Document'] = {}
        for key in docmatrix.reverseindex.keys():
            weight = docmatrix.get(key, key)
            if weight <= documentgraphconfig['nodethreshold'][1] and weight >= documentgraphconfig['nodethreshold'][0]:
                self.graph['nodes']['Document'][key] = docmatrix.get(key, key)


    def finalize(self):
        """
        final method compiling the graph and writing it to file
        """
        open(self.path, 'w+b').write(self.render( self.graph ))
        # returns relative path
        return self.path
