#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Apr, 12 2010$"

# core modules
import unittest
import os

from tinasoft import TinaApp
from tinasoft.data import Reader
from tinasoft.pytextminer import corpora
import pickle
import os

tinasoft = TinaApp(configFile='config.yaml',
            storage='tinabsddb://test.bsddb')

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.corpora = corpora.Corpora('test corpora')
        self.file = '/home/elishowk/TINA/Datas/pubmed_cancer_tina_toydb.txt'

    def testImporter(self):
        return
        count=0
        reader = Reader( "medline://"+self.file )
        fileGenerator = reader.parse()
        try:
            while 1:
                count += 1
                fileGenerator.next()
        except StopIteration, si:
            print "found %d documents"%count
            return

if __name__ == '__main__':
    unittest.main()
