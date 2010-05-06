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
            storage='tinabsddb://fetopen.bsddb')
        self.whitelist = self.tinasoft.get_whitelist(
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'user/100221-fetopen-filteredngrams.csv'
        )

    def test0GEXF(self):
        self.tinasoft.logger.debug( "testing tinasoft.data.gexf" )
        meta={
           'description' : "ngram GEXF graph test",
           'creators' : ['Julian bilcke', 'Elias Showk'],
        }

        opts={
            'DocumentGraph': {
                'threshold': [0, 'inf'],
            },
            'NGramGraph': {
                'threshold': [0.0, 1.0],
            }
        }
        gexf = Writer('gexf://', **opts)

        gexfstring = gexf.ngramDocGraph(
            self.tinasoft.storage,
            ['7','8','6','5'],
            meta,
            self.whitelist
        )
        #self.tinasoft.logger.debug(test)
        open("tinasoft_test.gexf", 'wb').write(gexfstring)

if __name__ == '__main__':
    unittest.main()
