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

import itertools
import math

from tinasoft.pytextminer.graph.matrix import *

import logging
_logger = logging.getLogger('TinaAppLogger')

class SubGraph(object):
    """
    Base class
    A simple subgraph proximity matrix processor
    """
    def __init__( self, config, storage, corpus, opts, name, ngram_index, doc_index ):
        """
        Attach storage, a corpus objects and init the Matrix
        """
        self.storage = storage
        self.corpusid = corpus['id']
        self.corpus = corpus
        self.name = name
        self._loadDefaults(config)
        self._loadOptions(opts)
        self.ngram_index = ngram_index
        self.doc_index = doc_index

    def _loadDefaults(self, config):
        """
        loads default from config
        """
        for attr, value in config['datamining'][self.name].iteritems():
            setattr(self,attr,value)

    def _loadOptions(self, opts):
        """
        Overwrites config with eventual user parameters
        """
        for attr, value in opts.iteritems():
            setattr(self,attr,value)

    def _loadProximity(self):
        _logger.debug("%s setting getsubgraph() to %s"%(self.name,self.proximity))
        self.getsubgraph = getattr(self, self.proximity)

    def getsubgraph(self, document):
        raise Exception("proximity method not defined in %s"%self.name)
        return 0

    def walkDocuments(self):
        """
        walks through a list of documents within a corpus
        """
        doccount = 0
        for doc_id in self.doc_index:
            obj = self.storage.loadDocument( doc_id )
            if obj is not None:
                yield obj
                doccount += 1
                self.notify( doccount )
            else:
                _logger.warning("%s cannot found document %s in database"%(self.name,doc_id))
        return

    def notify(self, doccount):
        if doccount % 20 == 0:
            _logger.debug("SubGraph.walkDocuments() processed %d percent of total documents"%round(100*(float(doccount)/float(len(self.doc_index)))))

class NgramGraph(SubGraph):
    """
    A NGram subgraph edges processor
    """
    def __init__(self, config, storage, corpus, opts, ngram_index, doc_index ):
        SubGraph.__init__(self, config, storage, corpus, opts, 'NGramGraph', ngram_index, doc_index )

    def getsubgraph( self, ngid ):
        """
        gets preprocessed proximities from storage and copies them to a Submatrix
        """
        submatrix = Matrix( list(self.ngram_index), valuesize=float )
        ngirow = self.storage.loadGraphPreprocess(self.corpusid+"::"+ngid, "NGram")

        if ngirow is None: return submatrix
        #_logger.debug("NGramGraph, value got from storage %s"%ngirow)

        for ngj, stored_prox in ngirow.iteritems():
            if ngj not in self.ngram_index: continue
            # does not replicate
            submatrix.set( ngid, ngj, stored_prox )

        return submatrix

    def diagonal( self, matrix_reducer ):
        for ng, weight in self.corpus['edges']['NGram'].iteritems():
            matrix_reducer.set( ng, ng, value=weight)

    def generator(self):
        """
        walks through a list of documents into a corpus
        to process proximities
        """
        count = 0
        for ngid in self.ngram_index:
            yield self.getsubgraph( ngid )
            count += 1
            if count % 50:
                _logger.debug("NgramGraph processed %d percent of total NGrams"%round(100*(float(count)/float(len(self.ngram_index)))))


class NgramGraphPreprocess(NgramGraph):
    """
    A NGram subgraph edges PRE processor
    """
    def getsubgraph( self, document ):
        """
        simple document cooccurrences matrix calculator
        """
        submatrix = Matrix( document['edges']['NGram'].keys(), valuesize=float )
        # only loop on half of the matrix === symmetric
        for (ngi, ngj) in itertools.combinations(document['edges']['NGram'].keys(), 2):
            # replicates the symmetric cooc value
            submatrix.set( ngi, ngj, value=1 )
            submatrix.set( ngj, ngi, value=1 )
        return submatrix

    def generator(self):
        """
        walks through a list of documents into a corpus
        to process proximities
        """
        docgenerator = self.walkDocuments()
        try:
            while 1:
                yield self.getsubgraph( docgenerator.next() )
        except StopIteration, si:
            return

class DocGraph(SubGraph):
    """
    A Document SubGraph edges processor
    'proximity' parameter that maps one of its method
    """
    def __init__( self, config, storage, corpus, opts, ngram_index, doc_index ):
        SubGraph.__init__(self, config, storage, corpus, opts, 'DocumentGraph', ngram_index, doc_index )
        # docgraph needs a custom proximity function
        self._loadProximity()
        # document's ngrams set lists caching
        self.documentngrams = {}
        for doc in self.doc_index:
            documentobj = self.storage.loadDocument(doc)
            self.documentngrams[doc] = set(documentobj['edges']['NGram'].keys())

    def sharedNGrams( self, document ):
        """
        number of ngrams shared by 2 documents
        """
        submatrix = Matrix( self.doc_index, valuesize=float )
        doc1ngrams = self.documentngrams[document['id']]
        for docid in self.documentngrams.keys():
            if docid != document['id']:
                doc2ngrams = self.documentngrams[docid]
                prox = float(len( doc1ngrams & doc2ngrams ))
                submatrix.set( document['id'], docid, value=prox, overwrite=True )
                submatrix.set( docid, document['id'], value=prox, overwrite=True )

        return submatrix

    def logJaccard( self, document ):
        """
        Jaccard-like similarity distance
        Invalid if summing values avor many periods
        """
        submatrix = Matrix( self.doc_index, valuesize=float )
        doc1ngrams = self.documentngrams[document['id']]

        for docid in self.documentngrams.keys():
            if docid != document['id']:
                
                doc2ngrams = self.documentngrams[docid]
                ngramsintersection = set(self.corpus['edges']['NGram'])
                ngramsunion = set(self.corpus['edges']['NGram'])
                ngramsintersection &= doc1ngrams & doc2ngrams
                ngramsunion &= doc1ngrams | doc2ngrams

                numerator = 0
                for ngi in ngramsintersection:
                    numerator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                denominator = 0
                for ngi in ngramsunion:
                    denominator += 1/(math.log( 1 + self.corpus['edges']['NGram'][ngi] ))
                weight = 0
                if denominator > 0:
                    weight = float(numerator) / float(denominator)

                # write edge weight to submatrix
                submatrix.set(document['id'], docid, value=weight, overwrite=True)
                submatrix.set(docid, document['id'], value=weight, overwrite=True)
        return submatrix

    def diagonal( self, matrix_reducer ):
        for doc, weight in self.corpus['edges']['Document'].iteritems():
            matrix_reducer.set( doc, doc, value=weight)

    def generator(self):
        """
        uses SubGraph.walkDocuments() to yield processed proximities
        """
        generator = self.walkDocuments()
        try:
            while 1:
                document = generator.next()
                yield self.getsubgraph( document )
                #### CAUTION : this limits processing to an half of the matrix
                del self.documentngrams[document['id']]
        except StopIteration:
            return
