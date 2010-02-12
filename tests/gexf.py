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
from tinasoft.data import Writer

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://fetopen.test.bsddb')


    def testCountCooc(self):
        self.tinasoft.logger.debug( "Test : Starting selectCorpusCooc('1')" )
        test = Writer('gexf://').coocGraph(self.tinasoft.storage, corpusID='1',min=20,max=1500)
        self.tinasoft.logger.debug(test)

if __name__ == '__main__':
    unittest.main()
