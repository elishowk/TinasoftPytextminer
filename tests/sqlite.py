#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 04 2010 5:29:16 PM$"

# core modules
import unittest
import shutil

from tinasoft import TinaApp

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        shutil.copy( 'tests/fetopen.db', 'tests/test.db' )
        self.tinasoft = TinaApp(storage='tests/test.db')
        return

    def testRead(self):
        print self.tinasoft.storage.loadCorpora( u'fet open' )
        print self.tinasoft.storage.loadCorpus( u'1' )
        print self.tinasoft.storage.loadDocument( u'200440' )
        print self.tinasoft.storage.loadNGram( u'1837760514' )
        # corpus 2
        rep = self.tinasoft.storage.fetchCorpusNGram( u'2' ).fetchall()
        self.assertEqual( len(rep), 2333)
        # corpus 2
        rep = self.tinasoft.storage.fetchCorpusNGramID( u'2' ).fetchall()
        self.assertEqual( len(rep), 5123)
        # document 200440
        rep = self.tinasoft.storage.fetchDocumentNGram( u'200440' ).fetchall()
        self.assertEqual( len(rep), 144)
        rep = self.tinasoft.storage.fetchDocumentNGramID( u'200440' ).fetchall()
        self.assertEqual( len(rep), 144)
        # corpus 3
        rep = self.tinasoft.storage.fetchCorpusDocumentID( u'3' ).fetchall()
        self.assertEqual( len(rep), 110)

if __name__ == '__main__':
    unittest.main()
