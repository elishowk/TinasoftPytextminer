#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jul, 23 2010"

# core modules
import unittest
from tinasoft.pytextminer import stemmer

tokens = ["I'm","a","testing","sentence",".","And","I","Should","work out","despite","I","am","malformed"]
verifTokens = ["I'm", 'a', 'test', 'sentenc', '.', 'And', 'I', 'Should', 'work out', 'despit', 'I', 'am', 'malform']

class TestsTestCase(unittest.TestCase):

    def test(self):
        """instance of stemmer"""
        st = stemmer.Nltk()
        self.assertEqual( [st.stem(word) for word in tokens], verifTokens)

if __name__ == '__main__':
    unittest.main()
