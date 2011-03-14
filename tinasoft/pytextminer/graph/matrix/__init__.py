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
from collections import defaultdict
import operator

import logging
_logger = logging.getLogger('TinaAppLogger')

def defaultdict_factory(valuetype):
    return itertools.repeat(defaultdict(valuetype)).next


class Matrix(object):
    """
    Testing Prototype
    """
    def __init__(self, index, valuesize=float):
        """
        includes a defaultdict which automatically creates rows
        of nested defaultdict of integers values
        """
        #self.matrix = defaultdict(defaultdict_factory(valuesize))
        self.matrix = {}

    def get( self, key1, key2=None ):
        """
        Getter returning an entire row or a cell value
        """
        if key2 is None:
            if key1 in self.matrix:
                return self.matrix[key1]
            else:
                return None
        else:
            if key1 in self.matrix and key2 in self.matrix[key1]:
                return self.matrix[key1][key2]
            else:
                return None

    def set( self, key1, key2, value=1, overwrite=False ):
        """
        Setter using sums to increment cooc values
        """
        if overwrite is False:
            if key1 not in self.matrix:
                self.matrix[key1] = {}
                self.matrix[key1][key2] = value
                return
            elif key2 not in self.matrix[key1]:
                self.matrix[key1][key2] = value
            else:
                self.matrix[key1][key2] += value
        else:
            if key1 not in self.matrix: self.matrix[key1] = {}
            self.matrix[key1][key2] = value


class MatrixReducer(Matrix):
    """
    Generic matrix additioner
    One must use at least MatrixReducerFilter to get filtered graphs
    """
    def __init__(self, index, valuesize=float):
        Matrix.__init__(self, index, valuesize=valuesize)

    def add(self, submatrix):
        """"callback adding a submatrix into the self-contained matrix"""
        if submatrix is not None:
            for key1, subrow in submatrix.matrix.iteritems():
                for key2, subvalue in subrow.iteritems():
                    self.set(key1, key2, value=subvalue, overwrite=False)

    def extract_matrix(self, config, **kwargs):
        """
        yields all rows from the matrix in a dictionary form
        filtering only nodes at this step (edges in a second step)
        yielded IDs are real tinasoft storage IDs
        """
        minnode = float(config['nodethreshold'][0])
        maxnode = float(config['nodethreshold'][1])
        for key1, row in self.matrix.iteritems():
            # node key1 filter
            if key1 in row:
                if row[key1] < minnode or row[key1] > maxnode:
                    continue
            valid_row = {}
            # node key2 filter
            for key2, value in row.iteritems():
                if self.matrix[key2][key2] < minnode or self.matrix[key2][key2] > maxnode:
                    continue
                if key2 == key1: continue
                # converts any numpy.float to python float
                valid_row[key2] = float(value)
            if len(valid_row.keys()) > 0:
                yield (key1, valid_row)
            else:
                continue


class MatrixReducerFilter(MatrixReducer):
    """
    Simple matrix reducer : Only filtering edges values from MatrixReducer
    use it if there's no second level proximity to calculate
    """
    def extract_matrix( self, config, **kwargs ):
        """
        Gets row of filtered nodes and yields row of filtered edges
        """
        matrix = super(MatrixReducerFilter, self).extract_matrix( config )
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        #if 'hapax' in config:
        #    minedges = float(config['hapax'])
        try:
            count = 0
            while 1:
                validrow = {}
                nodei, row = matrix.next()
                for nodej in row.keys():
                    prox = row[nodej]
                    if maxedges is None and prox >= minedges:
                        validrow[nodej] = row[nodej]
                    elif prox <= maxedges and prox >= minedges:
                        validrow[nodej] = prox
                count += len(validrow.keys())
                if len(validrow.keys()) > 0:
                    yield (nodei, validrow)
                else:
                    continue
        except StopIteration, si:
            _logger.debug("MatrixReducerFilter generated %d valid edges"%count)

class MatrixReducerMaxDegree(MatrixReducerFilter):
    """
    Simple matrix reducer : Only filtering edges values from MatrixReducer
    use it if there's no second level proximity to calculate
    """
    def extract_matrix( self, config, **kwargs ):
        """
        Gets row of filtered nodes and yields row of filtered edges
        """
        matrix = super(MatrixReducerMaxDegree, self).extract_matrix( config )
        maxdegree = config['maxdegree']
        try:
            while 1:
                nodei, filteredrow = matrix.next()
                if len(filteredrow.keys()) <= maxdegree:
                    yield (nodei, filteredrow)
                    continue
                sortedtuples = sorted(filteredrow.iteritems(), key=operator.itemgetter(1), reverse=True)
                validrow = dict( sortedtuples[:maxdegree] )
                #_logger.debug("document %s have %d neighbours, weights between %f and %f"%\
                #    (nodei, len(validrow.keys()), sortedtuples[0][1], sortedtuples[-1][1]))
                if len(validrow.keys())>0:
                    yield (nodei, validrow)
        except StopIteration, si:
            return

class Cooccurrences(MatrixReducerFilter):
    """
    nothing more than a MatrixReducerFilter
    """
    pass


class PseudoInclusion(MatrixReducer):
    """
    Matrix Reducer used to store cooccurrence matrix,
    then extract pseudo-inclusion on the fly
    """
    def extract_matrix( self, config, **kwargs ):
        alpha = config['alpha']
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        matrix = super(PseudoInclusion, self).extract_matrix( config )
        try:
            count = 0
            while 1:
                nodei, row = matrix.next()
                occi = self.get(nodei, nodei)
                for nodej in row.keys():
                    cooc = row[nodej]
                    occj = self.get(nodej, nodej)
                    # calculates the pseudo-inclusion prox
                    value = (float( cooc / occi )**float(alpha)) * (float( cooc / occj )**float(1/alpha))
                    if maxedges is None and value < minedges:
                        del row[nodej]
                    elif value > maxedges or value < minedges:
                        del row[nodej]
                    else:
                        row[nodej] = value
                count += len(row.keys())
                if len(row.keys()) > 0:
                    yield (nodei, row)
                else:
                    continue
        except StopIteration:
            _logger.debug("PseudoInclusionMatrix generated %d valid edges"%count)


class EquivalenceIndex(MatrixReducer):
    """
    Implements Equivalence Index distance between two NGram nodes
    based on the mutual information of two NGrams
    """
    def extract_matrix( self, config, **kwargs ):
        """
        extract_matrix with an equivalence index proximity calculator
        """
        nb_documents = config['nb_documents']
        minedges = float(config['edgethreshold'][0])
        maxedges = float(config['edgethreshold'][1])
        matrix = super(EquivalenceIndex, self).extract_matrix( config )
        try:
            count = 0
            while 1:
                nodei, row = matrix.next()
                occi = self.get(nodei, nodei)
                for nodej in row.keys():
                    cooc = row[nodej]
                    occj = self.get(nodej, nodej)
                    # calculates the e-index
                    brut = float(cooc * nb_documents) / float(occi * occj)
                    if brut <= 0: continue
                    value = math.log( brut )
                    if maxedges is None and value < minedges:
                        del row[nodej]
                    elif value > maxedges or value < minedges:
                        del row[nodej]
                    else:
                        row[nodej] = value
                count += len(row.keys())
                if len(row.keys()) > 0:
                    yield nodei, row
                else:
                    continue
        except StopIteration:
            _logger.debug("EquivalenceIndexMatrix generated %d valid edges"%count)