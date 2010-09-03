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

from tinasoft.pytextminer import ngram, cooccurrences, tokenizer
from tinasoft.data import Reader

# traceback to print error traces
import traceback
import time
#import os
from numpy import *


import logging
_logger = logging.getLogger('TinaAppLogger')

class SymmetricMatrix():
    """
    subclass of cooccurrences.CoocMatrix
    used only for semi matrix
    SymmetricMatrix.set(key1,key2,boolean,boolean) === SymmetricMatrix.set(key2,key1,boolean,boolean)
    SymmetricMatrix.get(key1,key2) === SymmetricMatrix.get(key2,key1)
    DO NOT increment cooccurrence twice !!!
    """
    def __init__(self, size, type=int32):
        self.size = size
        self.matrix = zeros((size,size), dtype=int32)
        self.index = []

    def _getindex(self, key):
        """
        incremental index handler
        given an external key, gives the internal key
        """
        if key in self.index:
            return self.index.index(key)
        else:
            self.index.append(key)
            return len(self.index) - 1

    def get( self, key1, key2=None ):
        """
        Getter returning rows or cell from the array
        """
        if key2 is None:
            return self.matrix[ self._getindex(key1), : ]
        else:
            indices = [self._getindex(key1), self._getindex(key2)]
            indices.sort()
            return self.matrix[ indices[1], indices[0] ]

    def set( self, key1, key2, bool1, bool2, value=None ):
        """
        Increments cooc array using boolean multiplication
        Sort keys and avoid duplicates values.
        """
        indices = [self._getindex(key1), self._getindex(key2)]
        indices.sort()
        if value is None:
            self.matrix[ indices[1], indices[0] ] += bool1*bool2
        else:
            self.matrix[ indices[1], indices[0] ] += value

    def walk_matrix(self, minCooc=1):
        """
        yields all values of the semi-matrix
        """
        countcooc = 0
        for i in range(self.size):
            if i <= len(self.index) -1:
                ngi = self.index[i]
                row = {}
                line = self.matrix[i,:]
                for j in range(i+1):
                    cooc = line[j]
                    if cooc >= minCooc:
                        countcooc += 1
                        if j <= len(self.index) - 1:
                            ngj = self.index[j]
                            row[ngj] = line[j]
                yield (ngi, row)
        _logger.debug("found %d non-zeros cooccurrences values"%countcooc)


class ArchiveCounter():
    """
    A cooccurrences matrix processor for large archives given a whitelist
    """
    def __init__(self, storage):
        # target database
        self.storage = storage

    def _notify(self, articleNumber):
        if not articleNumber%1000 :
            _logger.debug( "ArchiveCounter executed on %d abstracts at %s"%(articleNumber,time.asctime(time.localtime())) )

    def walkCorpus(self, whitelist, reader, period, exporter=None, minCooc=1):
        """
        parses a file, with documents for one period
        and processes cooc for a given whitelist
        """
        # loads a whitelist
        descriptorNameList, termDictList = self._load_whitelist(whitelist)
        # basic counter
        articleNumber = 0
        # size of the whitelist
        nDescriptors = len(descriptorNameList)
        # creates a cooc array
        self.matrix = SymmetricMatrix(nDescriptors)
        # starts parsing file
        try:
            while 1:
                # Scan input file (loop broken if end-of-file reached, see below)
                abstract, period = reader.next()
                articleNumber += 1
                self._notify(articleNumber)
                # words list
                wordSequence = tokenizer.RegexpTokenizer.tokenize(
                    tokenizer.TreeBankWordTokenizer.sanitize(abstract['content'])
                )
                # Occurrences
                currentDescriptors, markerList = self._occurrences(termDictList, wordSequence, nDescriptors)
                # Cooccurrences
                self._cooccurrences(descriptorNameList, currentDescriptors, markerList)
        except StopIteration, si:
            if exporter:
                exporter.export_matrix(self.matrix, period, minCooc)
            return True
        except Exception, exc:
            return False


    def _load_whitelist(self, whitelist):
        """
        transforms a whitelist object to descriptorNameList and termDictList
        *** example values :
        descriptorNameList = ['brain', 'cell', 'neuron', 'pain threshold', 'long term memory', 'mind theory']
        termDictList = [{'brains': 0, 'brain': 0, 'cell': 1, 'neuron': 2, 'neurons' : 2, 'neuronal' : 2}, {'pain threshold' : 3, 'mind theory' : 5 }, {'long term memory' : 4}]

        """
        termNameList = []
        termDictList = []
        maxLength = 0

        # walks through all ngram in the whitelist
        for ngid in whitelist['content'].iterkeys():
            # check if the ng is whitelisted
            if whitelist.test(ngid):
                ngobj = whitelist['content'][ngid]
                # puts the major form into termNameList
                termNameList.append(ngobj['label'])
                if len(termDictList) < len(ngobj['content']):
                    # auto-adjust termDictList size
                    for i in range(len(termDictList),len(ngobj['content'])):
                        termDictList.append({})
                # insert terms and forms into termDictList
                index = len(termNameList) - 1
                termDictList[len(ngobj['content'])-1][ngobj['label']] = index
                for label in ngobj['edges']['label'].iterkeys():
                    termDictList[len(label.split(" "))-1][label] = index
        termNameList.sort()
        return termNameList, termDictList

    def _occurrences(self, termDictList, wordSequence, nDescriptors):
        """
        Constitue NGrams and detects occurrences and mark corresponding descriptors
        """
        # array of boolean terms presence markers with the same index than termDictList
        markerList = zeros((nDescriptors), dtype = bool_)
        markerList[:] = False
        # list of term index present in the document (correspondingDescriptorNumber)
        currentDescriptors = []
        for termWordLengthMinusOne in range(len(termDictList)):
            for i in range(len(wordSequence) - termWordLengthMinusOne):
                # extract ngrams into wordWindow
                if i == 0 :
                    wordWindow = wordSequence[:termWordLengthMinusOne + 1]
                else :
                    wordWindow.append(wordSequence[i + termWordLengthMinusOne])
                    wordWindow.pop(0)
                currentTerm = ' '.join(wordWindow)
                # mark presence of an ngram in the document
                if termDictList[termWordLengthMinusOne].has_key(currentTerm):
                    correspondingDescriptorNumber = termDictList[termWordLengthMinusOne][currentTerm]
                    if not markerList[correspondingDescriptorNumber]:
                        markerList[correspondingDescriptorNumber] = True
                        currentDescriptors.append(correspondingDescriptorNumber)
        return currentDescriptors, markerList

    def _cooccurrences(self, descriptorNameList, currentDescriptors, markerList):
        """
        increments the cooccurrences matrix
        using a boolean multiplication on the ngram markerList
        """
        nCurrentDescriptors = len(currentDescriptors)
        for i in range(nCurrentDescriptors):
            l = currentDescriptors[i]
            for j in range(nCurrentDescriptors-i):
                c = currentDescriptors[i+j]
                # l and c are the ngram index from descriptorNameList
                self.matrix.set(
                    ngram.NGram.getId(descriptorNameList[l].split(" ")),
                    ngram.NGram.getId(descriptorNameList[c].split(" ")),
                    markerList[l],
                    markerList[c]
                )

    def writeMatrix(self, period, overwrite=True, minCooc=1):
        """
        stores into Cooc DB table of the SEMI matrix
        'corpus::ngramid' => '{ 'ngx' : y, 'ngy': z }'
        where ngramid <= ngi
        """
        generator = self.matrix.walk_matrix(minCooc)
        key = period+'::'
        try:
            while 1:
                ngi, row = generator.next()
                if len( row.keys() ) > 0:
                    self.storage.updateCooc( key+ngi, row, overwrite )
        except StopIteration, si:
            self.storage.flushCoocQueue()
            return True
        except Exception, exc:
            _logger.error( traceback.format_exc() )
            return False


    def readMatrix(self, size, period):
        """
        OBSOLETE : TO UPDATE
        """
        matrix = SymmetricMatrix(size)
        try:
            generator = self.storage.selectCorpusCooc( period )
            while 1:
                ngi,row = generator.next()
                for ngj in row.iterkeys():
                    matrix.set( ngi, ngj, value=row[ngj] )
        except StopIteration, si:
            return matrix
