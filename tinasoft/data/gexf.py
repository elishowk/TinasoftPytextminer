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
            path,
            db,
            periods,
            meta={},
            whitelist=None,
            ngramgraphconfig=None,
            documentgraphconfig=None
        ):
        """
        uses Cooc from database to write a NGram & Document graph for a given list of periods
        using Cooccurencesfor NGram proximity computing
        """
        if len(periods) == 0: return
        count = 0
        # init graph object
        ngramDocGraph = graph.Graph()
        ngramDocGraph.gexf.update(meta)
        # init subgraphs objects
        ngramGraph = graph.NGramGraph(
            db.loadNGram,
            ngramgraphconfig,
            self.NGramGraph
        )
        docGraph = graph.DocumentGraph(
            db.loadDocument,
            documentgraphconfig,
            self.DocumentGraph,
            whitelist=whitelist
        )
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
                docGraph.addNode( ngramDocGraph, doc_id, occ )
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
                    ngramGraph.addNode( ngramDocGraph, ngid1, occ1 )
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
                        ngramGraph.addNode( ngramDocGraph, ngid2, 0 )
                        # cooccurrences is the temporary edge's weight
                        ngramGraph.addEdge( ngramDocGraph, ngid1, ngid2, cooc, 'directed', False )

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
        ngramGraph.mapNodes( ngramDocGraph )
        ngramGraph.mapEdges( ngramDocGraph )
        docGraph.mapNodes( ngramDocGraph )
        docGraph.mapEdges( ngramDocGraph )
        # stores edges in database
        ngramGraph.cache.update( docGraph.cache )
        # empty db object cache
        docGraph.cache = {}
        self._updateEdgeStorage( db, ngramGraph.cache )
        ngramGraph.cache = {}
        # remove edges from the graph
        ngramDocGraph.gexf['edges'] = {}
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
