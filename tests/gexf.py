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
        self.whitelist = self.tinasoft.getWhitelist(
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'user/100221-fetopen-filteredngrams.csv'
        )

    def test0GEXF(self):
        self.tinasoft.logger.debug( "testing tinasoft.data.gexf" )
        meta={
           'description' : "ngram GEXF graph test",
           'creators' : ['Julian bilcke', 'Elias Showk'],
        }
        threshold=[0.7, 1.0]
        gexfstring = Writer('gexf://').ngramDocGraph(
            self.tinasoft.storage,
            ['1','2'],
            threshold,
            meta,
            self.whitelist
        )
        #self.tinasoft.logger.debug(test)
        open("tina_%s-%s.gexf"%(threshold[0],threshold[1]), 'wb').write(gexfstring)

if __name__ == '__main__':
    unittest.main()
