#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010"

# core modules
import unittest
import os
import random

from tinasoft.data import Reader
from tinasoft.pytextminer import tagger

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.csv = Reader("basecsv://tests/data/tagger.csv")

    def testPOSTag(self):
        """checks tagger response"""
        #sentenceTokens = self.sentenceMock
        pos = tagger.posTag(sentenceTokens)
        # check pos

    def testGetcontent(self):
        """checks tagged tokens format"""

if __name__ == '__main__':
    unittest.main()
