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


    def test0GEXF(self):
        self.tinasoft.logger.debug( "Test : GEXF on corpus 1" )
        test = Writer('gexf://').coocDistanceGraph(
        db=self.tinasoft.storage, 
        corpus=1,
        threshold=[2.0,3.0],
        meta={
           'description' : "ngram graph on corpus 1",
           'creators' : ['Julian bilcke', 'Elias Showk'],
        })
        self.tinasoft.logger.debug(test)

if __name__ == '__main__':
    unittest.main()
