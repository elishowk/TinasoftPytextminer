#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010"

# core modules
import unittest
import os
import random
from tinasoft.pytextminer import *
import pickle

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.cursor = pickle.load( open('tests/data/ngram-dbcursor.pickle', 'r') )

    def testAll(self):
        filtertag = ngram.PosTagFilter()
        filtercontent = ngram.Filter()
        validtag = ngram.PosTagValid()
        try:
            count=0
            countFilterContent =0
            countFilterTag = 0
            countValid = 0
            for record in self.cursor:
                count+=1
                if filtertag.test( record[1] ) is False:
                    countFilterTag += 1
                if filtercontent.test( record[1] ) is False:
                    countFilterContent += 1
                if validtag.test( record[1] ) is False:
                    countValid += 1
            raise StopIteration()
        except StopIteration, si:
            print "content filters eliminated = ", countFilterContent
            print "POS tag filter eliminated = ", countFilterTag
            print "POS tag VALIDATOR eliminated = ", countValid
            print "Total NGrams = ", count
            self.assertEqual( countFilterContent, 4 )
            self.assertEqual( countFilterTag, 11 )
            self.assertEqual( count, 50 )

if __name__ == '__main__':
    unittest.main()
