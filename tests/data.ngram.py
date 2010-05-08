#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Feb, 19 2010 5:29:16 PM$"

# core modules
import unittest
import os

from tinasoft import TinaApp
from tinasoft.pytextminer import stopwords, ngram
from tinasoft.data import ngram

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://test.bsddb')

    def testImportWhitelists(self):
        handle = ngram.WhitelistHandler()
        handle.walk("/home/elishowk/TINA/Datas/MedlineCancer/intermed-whitelists/")

    def testImportNGrams(self):
        return
        whitelist = self.tinasoft.importNGrams(
            'tests/test-importNGrams.csv',
            occsCol='occurrences',
        )
        self.tinasoft.logger.debug(whitelist)

    def testExportNGrams(self):
        return
        def generate():
            corpusgenerator = self.tinasoft.storage.select('Corpus')
            try:
                while 1:
                    corp=corpusgenerator.next()
                    yield corp[1], 'fet open'
            except StopIteration, si:
                return
        stopwd = stopwords.StopWords( "file://%s" % self.tinasoft.config['stopwords'] )
        filtertag = ngram.PosTagFilter()
        filterContent = ngram.Filter()
        filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )
        filters=[stopwd,filtertag,filterContent,filterstop]
        synthesispath = '100218-fetopen-corpora-synthesis.csv'
        mergepath = '100218-fetopen-ngrams.csv'
        generator = generate()
        self.tinasoft.exportNGrams(generator, synthesispath, filters=filters, mergepath=mergepath)

    def testExportAllNGram(self):
        return
        filepath = '100215-fetopen-ngrams.txt'
        self.tinasoft.exportAllNGrams(filepath)

    def testReadCorpus(self):
        """export ngrams for each """
        return
        json = self.tinasoft.getAllCorpus(raw=True)
        #self.tinasoft.logger.debug(json)
        for corpus in json:
            filepath = '100215-corpus_'+corpus['id']+'-ngrams.txt'
            self.tinasoft.exportCorpusNGram(corpus, filepath)

    def testExportCorpusCooc(self):
        return
        json = self.tinasoft.getAllCorpus(raw=True)
        #self.tinasoft.logger.debug(json)
        for corpus in json:
            filepath = '100215-corpus_'+corpus['id']+'-cooccurrences.txt'
            self.tinasoft.exportCorpusCooc(corpus, filepath, delimiter=" ")

if __name__ == '__main__':
    unittest.main()
