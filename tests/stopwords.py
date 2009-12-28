#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import codecs

# third party modules
#import yaml

from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_unit(self):
        stop = stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        print stop
        testngram=['i', 'am', 'a', 'test']
        stop.add( testngram )
        stop.savePickle( 'tests/stopwords_save_test.pickle' )
        fromPickle = stopwords.StopWords( arg="pickle://tests/stopwords_save_test.pickle" )
        self.assertEqual( fromPickle.contains(testngram), True )


if __name__ == '__main__':
    unittest.main()
