#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 15 2010 5:29:16 PM$"

# core modules
import unittest

from tinasoft import TinaApp

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(storage='tinabsddb://tests/test.bsddb')

    def testRead(self):
        pass

if __name__ == '__main__':
    unittest.main()
