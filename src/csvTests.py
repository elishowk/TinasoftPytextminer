#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer

class TestDataFET(unittest.TestCase):
    def setUp(self):
        pass

    def test_proposal(self):
        fet = Reader("fet://t/data-proposal.csv", 'data-proposal') 
        print fet.corpus
 
if __name__ == '__main__':
    unittest.main()
