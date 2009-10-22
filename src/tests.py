#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

# textminer modules
from textminer import *

class  TestsTestCase(unittest.TestCase):
    def setUp(self):
        try:
            f = open("src/t/testdata.yml")
        except:
            f = open("t/testdata.yml")
        self.data = yaml.load(f)
        f.close()
    
    def test_tests(self):
        for test in self.data['tests']:
            target = Target(test['original'])
            target.run()
            self.assertEqual(target.ngrams, test['ngrams'], test['test'])
 
if __name__ == '__main__':
    unittest.main()

