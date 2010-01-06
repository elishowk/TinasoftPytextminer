#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"

# core modules
import os
import unittest
import yaml
import locale
from tinasoft.data import Reader, Writer, Engine
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        # import config yaml
        try:
            self.options = yaml.safe_load( file( "config.yaml", 'rU' ) )
        except yaml.YAMLError, exc:
            print "\nUnable to read ./config.yaml file : ", exc
            return 
        try:
            self.locale = self.options['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            print "locale %s was not found, switching to en_US.UTF-8 by default", self.options['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        self.indexer =  indexer.TinaIndex( "tests/" )

	def testRun(self):
		csvinputpath = "tina://%s"%self.options['input']
		tinaImporter = Reader(csvinputpath,
			delimiter = self.options['delimiter'],
			quotechar = self.options['quotechar'],
			locale = self.locale,
			fields = self.options['fields']
		)
		corps = corpora.Corpora( name=self.options['name'] )
		corps = tinaImporter.corpora( corps )
		# first indexation
		self.indexer.indexDocs( tinaImporter.docDict )
		# second indexation with overwrite=False (default)
		notIndexedTwoTimes = self.indexer.indexDocs( tinaImporter.docDict.values() )
		len( notIndexedTwoTimes )
		for doc in notIndexedTwoTimes:
			compare = ( doc in tinaImporter.docDict.values() )
			self.assertEqual( compare, True )

if __name__ == '__main__':
    os.system('rm -rf tests/_MAIN_*')
    unittest.main()

