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



class Exporter (GEXFHandler):
    """
    A Gexf Exporter engine providing various graph construction methods
    """

    def notify(self, count):
        if count % 100 == 0:
            _logger.debug( "%d graph nodes processed"%count )

    def ngramDocGraph(
            self,
            coocmatrix,
            docmatrix,
            path,
            db,
            periods,
            meta={},
            whitelist,
            ngramgraphconfig=None,
            documentgraphconfig=None
        ):
        """
        uses loaded numpy arrays to write a NGram & Document graphs
        given a list of periods and a whitelist
        """
        # init graph object
        ngramDocGraph = graph.Graph()
        ngramDocGraph.gexf.update(meta)
        # init subgraphs objects
        ngramGraph = graph.NGramGraph(
            db.loadNGram,
            ngramgraphconfig,
            self.NGramGraph,
            whitelist
        )

        docGraph = graph.DocumentGraph(
            db.loadDocument,
            documentgraphconfig,
            self.DocumentGraph,
            whitelist
        )

        for period in periods:
            # loads the corpus (=period) object
            corp = db.loadCorpus(period)
            if corp is None:
                _logger.warning("Period %s not found, passing"%period)
                continue
            docGraph.load( docmatrix, ngramDocGraph, corp )
            ngramGraph.load( coocmatrix, ngramDocGraph, corp )


        #ngramGraph.mapNodes( ngramDocGraph )
        #ngramGraph.mapEdges( ngramDocGraph )
        #docGraph.mapNodes( ngramDocGraph )
        #docGraph.mapEdges( ngramDocGraph )
        # stores edges in database
        #ngramGraph.cache.update( docGraph.cache )
        # empty db object cache
        #docGraph.cache = {}
        #self._updateEdgeStorage( db, ngramGraph.cache )
        #ngramGraph.cache = {}
        # remove edges from the graph
        #ngramDocGraph.gexf['edges'] = {}
        # compiles then writes the gexf file
        open(path, 'w+b').write(self.render( ngramDocGraph.gexf ))
        # return relative path
        return path

    def _updateEdgeStorage( self, db, cache ):
        """
        Updates objects with the computed edges
        Then stores it into database
        """
        _logger.debug("will update graph edges into storage")
        for graphid in cache.keys():
            type, dbid = graphid.split("::")
            if type == 'NGram':
                db.insertNGram( cache[graphid], overwrite=True )
            elif type == 'Document':
                db.insertDocument( cache[graphid], overwrite=True )
            del cache[graphid]
