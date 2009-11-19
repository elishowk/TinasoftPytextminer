#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale
import random

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Writer

class TestSQLite(unittest.TestCase):
    def setUp(self):
        # try to determine the locale of the current system
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)

    def test_proposal(self):
        sql = Writer("sqlite://t/output/sqlite-database.db", locale=self.locale)
        sql.storeDocument( {"name":"lala", "targets" : [143234,2342,346356,467568] } )
        for x in xrange(800):
            sql.storeNGram( {"occs":random.randint(1,20), "ngram" : ["aaa","bbb","ccccccc","dddd","ii","uyuuououououou"] } )
        
if __name__ == '__main__':
    unittest.main()
