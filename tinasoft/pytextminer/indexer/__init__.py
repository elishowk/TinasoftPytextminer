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

from tinasoft.pytextminer import ngram, cooccurrences
from tinasoft.data import Reader
# traceback to print error traces
import traceback
import pickle
import time
#import os
from numpy import *
from os import access, W_OK, mkdir
from os.path import join

# *** Variables used for special characters removal ***
# allChars = string of all characters
# toKeep = string of characters to keep
# toStrip = string of characters to remove

from string import letters
toKeep = letters + ' ' + '-' + '0123456789'
allChars = "".join(map(chr, xrange(256)))
toStrip = "".join(c for c in allChars if c not in toKeep)


import logging
_logger = logging.getLogger('TinaAppLogger')


def createDirectory(directory):
    if not access(directory, W_OK):
        os.mkdir(directory)

class Matrix(cooccurrences.CoocMatrix):
    """
    subclass of cooccurrences.CoocMatrix
    """
    def set( self, key1, key2, bool1, bool2 ):
        """
        Increments cooc array using boolean multiplication, including the symetric value
        """
        self.array[self._getindex(key1),self._getindex(key2)] += bool1*bool2
        if key1 != key2:
            self.array[self._getindex(key2),self._getindex(key1)] += bool1*bool2
        print self.array[self._getindex(key1),self._getindex(key2)]

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

    def walkCorpus(self, whitelist, reader, outpath=None):
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
        # array of boolean terms presence markers with the same index than termDictList
        markerList = zeros((nDescriptors), dtype = bool_)
        # list of term index present in the document (correspondingDescriptorNumber)
        currentDescriptors = []
        try:
            while 1:
                # Scan input file (loop broken if end-of-file reached, see below)
                abstract, period = reader.next()
                articleNumber += 1
                self._notify(articleNumber)
                # words list
                wordSequence = abstract['content']\
                    .lower()\
                    .translate(allChars, toStrip)\
                    .replace("-", " ")\
                    .split(" ")
                # Occurrences
                currentDescriptors, markerList = self._occurrences(termDictList, wordSequence, markerList, currentDescriptors)
                # Cooccurrences
                self._cooccurrences(descriptorNameList, currentDescriptors, markerList)
                # Reset
                markerList[:] = False
                currentDescriptors = []
        except StopIteration, si:
            return True
        except Exception, exc:
            return False

    def _load_whitelist(self, whitelist):
        """
        transforms a whitelist object to descriptorNameList and termDictList
        """
        termNameList = []
        termDictList = []
        maxLength = 0
        # walks through all ngram in the whitelist
        for ngid in whitelist['content'].iterkeys():
            # check if the ng is whitelisted
            if whitelist.test(ngid):
                ngobj = whitelist['content'][ngid]
                if len(ngobj['content']) > maxLength:
                    maxLength = len(ngobj['content'])
                # puts the major form into termNameList
                termNameList.append(ngobj['label'])
        termNameList.sort()
        for i in range(maxLength):
            termDictList.append({})

        for ngid in whitelist['content'].iterkeys():
            if whitelist.test(ngid):
                # puts all the forms into termDictList
                index = termNameList.index(ngobj['label'])
                termDictList[len(ngobj['content'])-1][ngobj['label']] = index
                for label in ngobj['edges']['label'].iterkeys():
                    termDictList[len(label.split(" "))-1][label] = index

        return termNameList, termDictList

    def _occurrences(self, termDictList, wordSequence, markerList, currentDescriptors):
        """
        Detect occurring relevant terms and mark corresponding descriptors
        """
        for termWordLengthMinusOne in range(len(termDictList)):
            for i in range(len(wordSequence) - termWordLengthMinusOne):
                # form ngrams into wordWindow
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

    def writeMatrix(self, overwrite=True):
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
                    if cooc > 0:
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


class SerialCounter():
    """
    Fast cooccurrences processor
    """
    def serialCounter(self,
            whitelist,
            periods,
            dataPath,
            outPath,
            dataType,
        ):
        """
        SerialCounter main method : iterate on periods and calls countCooccurrences()
        """
        self.abstractsPath = dataPath
        self.resultsPath = join(outPath, whitelist.label)

        self.semNetResultFileName = join(self.resultsPath, str(periods[0]) + '-' + str(periods[1]) + '_semantic_network.txt')
        self.nArticlesFileName = join(self.resultsPath, str(periods[0]) + '-' + str(periods[1]) + '_articles_per_period.txt')

        for year in periods:
            self.countCooccurrences(whitelist, year, dataType)
        return True


    def countCooccurrences(self, whitelist, period, dataType):
        """
        Count occurrences and cooccurences of chosen terms in titles and abstracts
        of archive articles for a given period
        """

        _logger.debug( "SerialCounter.countCooccurences on period %s starting at %s"%(period,time.asctime(time.localtime())) )

        #periodNoSpace = period.strip()

        #tNamesAndTDictsFileName = self.termsPath + termListName + '_tNamesAndTDicts.list'
        #if not os.path.isfile(tNamesAndTDictsFileName):
        #    self.termListFile2TNamesAndTDictsFile(termListName)
        #    _logger.debug( "Dictionaries file derived from term list '" + termListName + "' created" )
        #else:
        #    _logger.debug( "Dictionaries file derived from term list '" + termListName + "' already exists" )

        # loads pickle files from self.termListFile2TNamesAndTDictsFile
        #tNamesAndTDictsFile = file(tNamesAndTDictsFileName, 'r')

        # loads WHITELIST
        descriptorNameList, termDictList = self.whitelist2TNamesAndTDictsFile(whitelist)
        #tNamesAndTDictsFile.close()

        abstractsFilePath = join(self.abstractsPath, period, period + '.txt')
         # Test : use test file 'cooctest.txt' instead
        abstractsFile = Reader( dataType + "://" + abstractsFilePath )
        abstractsGen = abstractsFile.parseFile()
        # *** Test values :
        #descriptorNameList = ['brain', 'cell', 'neuron', 'pain threshold', 'long term memory', 'mind theory']
        #termDictList = [{'brains': 0, 'brain': 0, 'cell': 1, 'neuron': 2, 'neurons' : 2, 'neuronal' : 2}, {'pain threshold' : 3, 'mind theory' : 5 }, {'long term memory' : 4}]

        nDescriptors = len(descriptorNameList)

        cooccurrenceArray = zeros((nDescriptors,nDescriptors), dtype = int32)
        markerList = zeros((nDescriptors), dtype = bool_)

        articleNumber = 0

        currentDescriptors = []
        try:
            while 1: # Scan input file (loop broken if end-of-file reached, see below)

                abstract, period = abstractsGen.next()
                if not articleNumber%1000 :
                    print "countCooccurrences executed on %d abstracts at %s"%(articleNumber,time.asctime(time.localtime()))

                articleNumber +=1

                titleAndAbstractWordsSequence = abstract['content'].lower().translate(allChars, toStrip).replace('-', ' ').split()

                # Detect occurring relevant terms and mark corresponding descriptors

                for termWordLengthMinusOne in range(len(termDictList)):
                    for i in range(len(titleAndAbstractWordsSequence) - termWordLengthMinusOne):
                        if i == 0 :
                            wordWindow = titleAndAbstractWordsSequence[:termWordLengthMinusOne + 1]
                        else :
                            wordWindow.append(titleAndAbstractWordsSequence[i + termWordLengthMinusOne])
                            wordWindow.pop(0)
                        currentTerm = ' '.join(wordWindow)

                        if termDictList[termWordLengthMinusOne].has_key(currentTerm):
                            correspondingDescriptorNumber = termDictList[termWordLengthMinusOne][currentTerm]
                            if not markerList[correspondingDescriptorNumber]:
                                markerList[correspondingDescriptorNumber] = True
                                currentDescriptors.append(correspondingDescriptorNumber)

                # Cooccurrences
                nCurrentDescriptors = len(currentDescriptors)

                for i in range(nCurrentDescriptors):
                    l = currentDescriptors[i]
                    for j in range(nCurrentDescriptors-i):
                        c = currentDescriptors[i+j]
                        cooccurrenceArray[l,c] += markerList[l]*markerList[c]
                        if l != c:
                            cooccurrenceArray[c,l] += markerList[l]*markerList[c]

                # Reset
                markerList[:] = False
                currentDescriptors = []

        except StopIteration, si:
            del abstractsFile

        createDirectory(self.resultsPath)
        # writes the cooccurrences matrix file
        semNetResultFile = open(self.semNetResultFileName, 'w+')
        for i in range(nDescriptors):
            for j in range(nDescriptors):
                if(str(cooccurrenceArray[i,j])!="0"):
                    semNetResultFile.write(str(i) + " " + str(j) + " " + str(cooccurrenceArray[i,j]) + " " + period + "\n")
        semNetResultFile.close()
        # writes the synthesis of the period to a file
        nArticlesFile = open(self.nArticlesFileName, 'w+')
        nArticlesFile.write(str(period)+" "+str(articleNumber)+"\r\n")
        nArticlesFile.close()

        print 'Finished at ' + time.asctime(time.localtime())
        print 'Processed %s articles from archive at %s'%articleNumber

        print 'Cooccurrences lines written to file ' + self.semNetResultFileName
        print 'Number of articles written to file ' + self.nArticlesFileName

    def whitelist2TNamesAndTDictsFile(self, whitelist):
        """
        Creates term list and dictionaries from plain text file
        and pickle them to filesystem
            termNameList = ['brain', 'cell', 'neuron', 'pain threshold', 'long term memory', 'mind theory']
            termDictList = [
                {'brains': 0, 'brain': 0, 'cell': 1, 'neuron': 2, 'neurons' : 2, 'neuronal' : 2},
                {'pain threshold' : 3, 'mind theory' : 5 },
                {'long term memory' : 4}
            ]

        """
        print "whitelist2TNamesAndTDictsFile"

        termNameList = []
        termDictList = []
        maxLength = 0
        # walks through all ngram in the whitelist
        for ngid in whitelist['content'].iterkeys():
            # check if the ng is whitelisted
            if whitelist.test(ngid):
                ngobj = whitelist['content'][ngid]
                if len(ngobj['content']) > maxLength:
                    maxLength = len(ngobj['content'])
                # puts the major form into termNameList
                termNameList.append(ngobj['label'])
        termNameList.sort()
        for i in range(maxLength - 1):
            termDictList.append({})

        for ngid in whitelist['content'].iterkeys():
            if whitelist.test(ngid):
                # puts all the forms into termDictList
                index = termNameList.index(ngobj['label'])
                termDictList[len(ngobj['content'])-1][ngobj['label']] = index
                for label in ngobj['edges']['label'].iterkeys():
                    termDictList[len(label.split())-1][label] = index

        return termNameList, termDictList
