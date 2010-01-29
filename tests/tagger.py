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
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://fetopen.bsddb')
        print tagger.TreeBankPosTagger()

if __name__ == '__main__':
    unittest.main()
