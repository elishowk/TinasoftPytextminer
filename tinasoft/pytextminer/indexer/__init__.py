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
from os import access, W_OK, mkdir
from os.path import join


import logging
_logger = logging.getLogger('TinaAppLogger')


#import pdb

def createDirectory(directory):
    if not access(directory, W_OK):
        os.mkdir(directory)

class Matrix(cooccurrences.CoocMatrix):
    """
    subclass of cooccurrences.CoocMatrix
    """
    def set( self, key1, key2, bool1, bool2 ):
        """
        Increments cooc array using boolean multiplication, including symmetric value
        """
        self.array[self._getindex(key1),self._getindex(key2)] += bool1*bool2
        if key1 != key2:
            self.array[self._getindex(key2),self._getindex(key1)] += bool1*bool2

class ArchiveCounter():
    """
    A cooccurrences matrix processor for large archives given a whitelist
    """
    def __init__(self, storage, corpusid):
        # target database
        self.storage = storage
        self.corpusid = corpusid

    def _notify(self, articleNumber):
        if not articleNumber%1000 :
            _logger.debug( "ArchiveCounter executed on %d abstracts at %s"%(articleNumber,time.asctime(time.localtime())) )

    def walkCorpus(self, whitelist, reader, exporter=None):
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
        self.matrix = Matrix(nDescriptors)
        # starts parsing file
        try:
            while 1:
                # Scan input file (loop broken if end-of-file reached, see below)
                #import pdb
                #pdb.set_trace()
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
                if articleNumber == 1000:
                    raise StopIteration()
        except StopIteration, si:
            if exporter:
                exporter.export_matrix(self.matrix, self.corpusid)
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
        increments the cooccurrences matrix with a boolean multiplication on the ngram markerList
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

    def writeMatrix(self, overwrite=True, minCooc=1):
        """
        writes in the db rows of the matrix
        'Cooc::corpus::ngramid' => '{ 'ngx' : y, 'ngy': z }'
        """
        try:
            key = self.corpusid+'::'
            countcooc = 0
            for ngi in self.matrix.reverse.iterkeys():
                row = {}
                for ngj in self.matrix.reverse.iterkeys():
                    cooc = self.matrix.get( ngi, ngj )
                    if cooc >= minCooc:
                        countcooc += 1
                        row[ngj] = cooc
                if len( row.keys() ) > 0:
                    self.storage.updateCooc( key+ngi, row, overwrite )
            _logger.debug( 'will store %d non-zero cooc values'%countcooc )
            self.storage.flushCoocQueue()
            _logger.debug( 'finished storing %d non-zero cooc values'%countcooc )
            return True
        except Exception, exc:
            _logger.error( traceback.format_exc() )
            return False


    def readMatrix( self, size ):
        """
        OBSOLETE : TO UPDATE
        """
        matrix = Matrix(size)
        try:
            generator = self.storage.selectCorpusCooc( self.corpusid )
            while 1:
                id,row = generator.next()
                for ngi in row.iterkeys():
                    matrix.set( id, ngi )
        except StopIteration, si:
            return matrix
