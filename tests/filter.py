#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010"

# core modules
import unittest
import os
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://fetopen.test.bsddb')

    def testAll(self):
        selectgenerator = self.tinasoft.storage.select('NGram')
        filtertag = ngram.PosTagFilter()
        filtercontent = ngram.Filter()
        try:
            count=0
            countFilter=0
            while count<50:
                filterrecord = selectgenerator.next()
                count+=1
                res = filtertag.all( filterrecord[1] ) and\
                    filtercontent.all( filterrecord[1] )
                if res is False:
                    countFilter += 1
            raise StopIteration()
        except StopIteration, si:
            print "all() filters eliminated = ", countFilter
            print "Total NGrams = ", count
            return

if __name__ == '__main__':
    unittest.main()
