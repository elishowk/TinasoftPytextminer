#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Apr, 12 2010$"

# core modules
import unittest
import os

from tinasoft.data import Reader
from tinasoft.pytextminer import corpora
import pickle

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.corpora = corpora.Corpora('test corpora')
        self.file = '/home/elishowk/TINA/Datas/pubmed_result_dopamine.txt'
        self.corpus7docs=['16594642','16462412','16462411']
        self.corpus23docs=['16462411']

    def testImporter(self):
        reader = Reader( "medline://"+self.file )
        fileGenerator = reader.parse()
        try:
            while 1:
                fileGenerator.next()
        except StopIteration, si:
            return

if __name__ == '__main__':
    unittest.main()
