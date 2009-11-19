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

    def test_basic(self):
        backend = Writer("sqlite://t/output/test-sqlite.db", format='json', locale=self.locale)
        docs = { 32000 : {"name":"lala", "targets" : (143234,2342,346356,467568) },
                 31000 : {"name":"lala", "targets" : (143234,2342,346356,467568) } }
        for key, value in docs.iteritems():
            backend.insert( key, value )
        assert backend.fetch_one( dict, 30000 ) is None
 
        

if __name__ == '__main__':
    unittest.main()
