#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_unit(self):
        stop = stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        print stop
        testngram = ['i', 'am', 'a', 'test']
        obj = stop.add( testngram )
        stop.savePickle( 'tests/stopwords_save_test.pickle' )
        del stop
        fromPickle = stopwords.StopWords( arg="pickle://tests/stopwords_save_test.pickle" )
        self.assertEqual( fromPickle.contains(obj), True )


if __name__ == '__main__':
    unittest.main()
