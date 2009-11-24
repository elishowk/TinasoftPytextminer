#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import csv
import codecs
import locale
# pytextminer modules
from PyTextMiner.Data.medline import Model


class TestMedlineToCSV(unittest.TestCase):
    def setUp(self):
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
            self.lang, self.encoding = self.locale.split('.')
            self.file = codecs.open("pubmed_result.txt", "rU", self.encoding)
            self.output = codecs.open("t/output/pubmed_result.csv", "w", self.encoding)
        except:
            print "error setting locale or reading file"
            pass
        
    def test1_import(self):
        writer = csv.writer(self.output, dialect='excel')
        rows = [['corp_num','doc_num','doc_titl','doc_acrnm','doc_abst','index1','index2']]
        lines=[]
        for line in self.file.readlines():
            line = line.rstrip()
            if line != "":
                lines.append(line)
                continue
            print lines
            model = Model(lines)
            try:
                    rows.append([abs(hash(model.stat)),model.pmid,model.pubname,model.author,
                        model.abstract,model.pmid,model.pmid])
                    lines=[]
            except:
                pass
        writer.writerows(rows) 

if __name__ == '__main__':
    unittest.main()
