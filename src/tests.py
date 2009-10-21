#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

import unittest
from textminer import *

class  TestsTestCase(unittest.TestCase):
    #def setUp(self):
    #    self.foo = Tests()
    #

    #def tearDown(self):
    #    self.foo.dispose()
    #    self.foo = None

    def test_tests(self):
        #assert x != y;
        #self.assertEqual(x, y, "Msg");
        #self.fail("TODO: Write test")
        target = Target("cécî èst un éssàï")
        target.run()

if __name__ == '__main__':
    unittest.main()

