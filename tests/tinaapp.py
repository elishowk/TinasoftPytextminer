#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest
import os
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://fetopen.test.bsddb')

    def testExportAllNGram(self):
        filepath = '100215-fetopen-ngrams.txt'
        self.tinasoft.exportAllNGrams(filepath)

    def testReadCorpus(self):
        """export ngrams for each """
        return
        json = self.tinasoft.getAllCorpus(raw=True)
        #self.tinasoft.logger.debug(json)
        for corpus in json:
            filepath = '100215-corpus_'+corpus['id']+'-ngrams.txt'
            self.tinasoft.exportCorpusNGram(corpus, filepath)

    def testExportCorpusCooc(self):
        return
        json = self.tinasoft.getAllCorpus(raw=True)
        #self.tinasoft.logger.debug(json)
        for corpus in json:
            filepath = '100215-corpus_'+corpus['id']+'-cooccurrences.txt'
            self.tinasoft.exportCorpusCooc(corpus, filepath, delimiter=" ")


if __name__ == '__main__':
    unittest.main()
