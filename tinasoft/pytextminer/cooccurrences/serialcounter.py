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
import tinasoft
from tinasoft.data import Reader

import pickle
import time
import os
from numpy import *
from os import access, W_OK
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
        mkdir(directory)

class SerialCounter():

    def serialCounter(self, termListName, firstYear, lastYear, mainPath, dataType, dataPath='Data', termsPath='Terms', abstractsPath='PubMed_Abstracts', resultsPath='Results'):
        """
        SerialCounter main method
        usage : myinstance.serialCounter('List_gene_coli',1970,1997)
        """
        dataPath = join(mainPath, dataPath)
        self.termsPath = join(dataPath, termsPath)
        self.abstractsPath = join(dataPath, abstractsPath)
        self.resultsPath = join(mainPath, resultsPath, termListName + '_cooccurrences')

        self.semNetResultFileName = join(self.resultsPath, str(firstYear) + '-' + str(lastYear) + '_semNet.txt')
        #open(self.semNetResultFileName,"w+").close()

        self.nArticlesFileName = join(self.resultsPath, str(firstYear) + '-' + str(lastYear)  + '_nbDoc.txt')
        #open(self.nArticlesFileName,"w+").close()

        #tNamesAndTDictsFileName = self.termsPath + termListName + '_tNamesAndTDicts.list'
        #os.remove(tNamesAndTDictsFileName)

        for i in range(lastYear - firstYear + 1):
            year = str(lastYear - i)
            _logger.debug( 'serialCounter on YEAR ' + year )
            self.countCooccurrences(termListName, year + '[dp]', firstYear, lastYear, dataType) # modifier si requete differente


    def countCooccurrences(self, termListName, period, firstYear, lastYear, dataType):
        """Count occurrences and cooccurences of chosen terms in titles and abstracts of Pubmed articles corresponding to given period"""

        _logger.debug( "pubmedCooccurenceCounter2::countCooccurences :" )
        _logger.debug( "Beginning (co)occurrence counting at " + time.asctime(time.localtime()) )

        periodNoSpace = period.replace(' ', '')

        tNamesAndTDictsFileName = self.termsPath + termListName + '_tNamesAndTDicts.list'
        if not os.path.isfile(tNamesAndTDictsFileName):
            self.termListFile2TNamesAndTDictsFile(termListName)
            _logger.debug( "Dictionaries file derived from term list '" + termListName + "' created" )
        else:
            _logger.debug( "Dictionaries file derived from term list '" + termListName + "' already exists" )

        # loads pickle files from self.termListFile2TNamesAndTDictsFile
        tNamesAndTDictsFile = file(tNamesAndTDictsFileName, 'r')
        # loads WHITELIST
        descriptorNameList, termDictList = pickle.load(tNamesAndTDictsFile)
        tNamesAndTDictsFile.close()

        abstractsFilePath = join(self.abstractsPath, 'Pubmed_' + periodNoSpace, 'Pubmed_' + periodNoSpace + '.txt')
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
        abstractNumber = 0
        titleNumber = 0

        currentDescriptors = []
        try:
            while 1: # Scan input file (loop broken if end-of-file reached, see below)

                abstract, period = abstractsGen.next()
                if not articleNumber%1000 :
                    print "countCooccurrences executed on %d abstracts at %s"%(articleNumber,time.asctime(time.localtime()))

                articleNumber +=1
                titleNumber +=1
                abstractNumber +=1

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

        # Write results
        print "Write results"
        createDirectory(self.resultsPath)
        year = periodNoSpace.split("[")[0]

        #semNetResultFileName = join(self.resultsPath + termListName + str(firstYear) + '-' + str(lastYear) + '_semNet.txt'
        semNetResultFile = open(self.semNetResultFileName, 'a+')
        for i in range(nDescriptors):
            for j in range(nDescriptors):
                if(str(cooccurrenceArray[i,j])!="0"):
                    semNetResultFile.write(str(i)+" "+str(j)+" "+str(cooccurrenceArray[i,j])+" "+year+"\n")
        semNetResultFile.close()
        #nArticlesFileName = self.resultsPath + termListName + str(firstYear) + '-' + str(lastYear)  + '_nbDoc.txt'
        nArticlesFile = open(self.nArticlesFileName, 'a+')
        nArticlesFile.write(str(year)+" "+str(articleNumber)+"\r\n")
        nArticlesFile.close()

        print 'Finished at ' + time.asctime(time.localtime())
        print 'Processed %s articles, %s titles, %s abstracts'%(articleNumber, titleNumber, abstractNumber)

        print 'Cooccurrences lines written to file ' + self.semNetResultFileName
        print 'Number of articles written to file ' + self.nArticlesFileName

    def termListFile2TNamesAndTDictsFile(self, termListName):
        """
        Creates term list and dictionaries from plain text file
        and pickle them to filesystem
            termNameList = ['brain', 'cell', 'neuron', 'pain threshold', 'long term memory', 'mind theory']
            termNDictList = [
                {'brains': 0, 'brain': 0, 'cell': 1, 'neuron': 2, 'neurons' : 2, 'neuronal' : 2},
                {'pain threshold' : 3, 'mind theory' : 5 },
                {'long term memory' : 4}
            ]

        """
        print "pubmedCooccurenceCounter2::termListFile2TNamesAndTDictsFile :"

        # Load terms from plain text file
        print '1. Load terms from plain text file'
        termListFile = open(join(self.termsPath, termListName + '.txt'), 'r')

        termListList = []
        maxTermnWords = 0
        for line in termListFile.readlines():
            termList = line.replace('-', ' ').split(',')
            for i, term in enumerate(termList):
                termList[i] = term.strip()
                termnWords = len(term.split())
                if maxTermnWords < termnWords :
                    maxTermnWords = termnWords
            termListList.append(termList)
        termListList.sort()
        termListFile.close()

        # Determine maximum word-length of a term

        #print '2. Determine maximum word-length of a term '
        #maxTermnWords = 0
        #for termList in termListList:
        #    for term in termList:
        #        termnWords = len(term.split())
        #        if maxTermnWords < termnWords :
        #            maxTermnWords = termnWords

        # Create as many term dictionaries as needed

        print '3. Create as many term dictionaries as needed'

        # termDictList is a size-sorted list of dictionariesngrams indexed by lemma id
        termDictList = []
        for i in range(maxTermnWords):
            termDictList.append({})

        # Create term list and fill-in term dictionaries

        print '4. Create term list and fill-in term dictionaries'
        termNameList = []

        for termList in termListList:
            termName = termList[0]
            termNameList.append(termList[0])
            for term in termList:
                termnWords = len(term.split())
                termDictList[termnWords-1][term] = len(termNameList) - 1

        # Write results to file

        print '5. Write results to file'
        tNamesAndTDictsFileName = self.termsPath + termListName + '_tNamesAndTDicts.list'
        tNamesAndTDictsFile = open(tNamesAndTDictsFileName, 'w')
        pickle.dump([termNameList, termDictList], tNamesAndTDictsFile)
        tNamesAndTDictsFile.close()
