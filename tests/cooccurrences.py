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

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://fetopen.bsddb')

    def testCorpus(self):
        corpus = '1'
        cooc = cooccurrences.Simple(corpus)
        cooc.walkCorpus(self.tinasoft.storage)
        cooc.writeDB(self.tinasoft.storage)
        generator = self.tinasoft.storage.select('Cooc::1')
        try:
            record = generator.next()
            while record:
                #self.assertEqual( isinstance(record[1], dict), True)
                print record
                record = generator.next()
        except StopIteration, si:
            print "testRead ended"
            return
        #print cooc.matrix

if __name__ == '__main__':
    unittest.main()
