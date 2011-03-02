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

__author__ = "elishowk@nonutc.fr"

from tinasoft.pytextminer import corpus

import logging
_logger = logging.getLogger('TinaAppLogger')

def process_ngram_subgraph(
        config,
        dataset,
        periods,
        ngram_index,
        doc_index,
        ngram_matrix_reducer,
        ngramgraphconfig,
        graphwriter,
        storage,
        ngram_graph_class
    ):
    """
    Generates ngram graph matrices from indexed dataset
    """
    for process_period in periods:
        ngram_args = ( config, storage, process_period, ngramgraphconfig, ngram_index, doc_index )
        adj = ngram_graph_class( *ngram_args )
        adj.diagonal(ngram_matrix_reducer)
        try:
            submatrix_gen = adj.generator()
            count = 0
            while 1:
                ngram_matrix_reducer.add( submatrix_gen.next() )
                count += 1
                yield count
        except Warning, warn:
            _logger.warning( str(warn) )
            return
        except StopIteration, si:
            _logger.debug("NGram sub-graph (%s) finished period %s"%(
                    ngram_matrix_reducer.__class__.__name__,
                    process_period.id
                )
            )
    load_subgraph_gen = graphwriter.load_subgraph( 'NGram', ngram_matrix_reducer, subgraphconfig=ngramgraphconfig)
    try:
        while 1:
            # yields the updated node count
            yield load_subgraph_gen.next()
    except StopIteration, si:
        # finally yields the matrix reducer
        yield ngram_matrix_reducer
        return

def process_document_subgraph(
        config,
        dataset,
        periods,
        ngram_index,
        doc_index,
        doc_matrix_reducer,
        docgraphconfig,
        graphwriter,
        storage,
        doc_graph_class
    ):
    """
    Generates document graph from an indexed dataset
    Merging multiple periods in one meta object
    """
    metacorpus = corpus.Corpus("meta")
    for process_period in periods:
        metacorpus.updateEdges(process_period.edges)
        #metacorpus = PyTextMiner.updateEdges(process_period.edges, metacorpus)

    doc_args = ( config, storage, metacorpus, docgraphconfig, ngram_index, doc_index )
    adj = doc_graph_class( *doc_args )
    adj.diagonal(doc_matrix_reducer)
    try:
        submatrix_gen = adj.generator()
        doccount = 0
        while 1:
            doc_matrix_reducer.add( submatrix_gen.next() )
            doccount += 1
            yield doccount
    except Warning, warn:
        _logger.warning( str(warn) )
        return
    except StopIteration, si:
        _logger.debug("Document sub-graph (%s) finished period %s"%(
                doc_matrix_reducer.__class__.__name__,
                metacorpus.id
            )
        )

    load_subgraph_gen = graphwriter.load_subgraph( 'Document', doc_matrix_reducer, subgraphconfig = docgraphconfig)
    try:
        while 1:
            # yields the updated node count
            yield load_subgraph_gen.next()
    except StopIteration, si:
        # finally yields the matrix reducer
        yield doc_matrix_reducer
        return


