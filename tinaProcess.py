#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__="Elias Showk"

#!/usr/bin/python
# -*- coding: utf-8 -*-

from tinasoft import TinaApp
from tinasoft.data import Engine, Writer
from tinasoft.pytextminer import *

class Program(TinaApp):
    def __init__(self):
        TinaApp.__init__(self, storage='tinabsddb://100125-fetopen.bsddb')
        # sqlite incoming connexion
        self.csv = Writer("basecsv://100125-fetopen-filter.csv")
        self.filtertag = tagger.PosFilter()

    def reprpostag(self, postagng):
        str = ""
        for word,tag in postagng:
            str += " "
            str += tag
        return str.strip()

    def exportAll(self):
        """export pos tag filtered ngrams"""
        selectgenerator = self.storage.select('NGram')
        both = self.filtertag.both(selectgenerator)
        any = self.filtertag.any(both)
        begin = self.filtertag.begin(any)
        end = self.filtertag.end(begin)
        count = 0
        ngrams=[]
        self.csv.writeRow(['id','label','pos_tag'])
        try:
            filterrecord = end.next()
            #print filterrecord
            count += 1
            while filterrecord:
                ngrams += [filterrecord[1]['id']]
                tag = self.reprpostag( filterrecord[1]['postag'] )
                self.csv.writeRow([ filterrecord[1]['id'], \
                    filterrecord[1]['label'], \
                    tag
                    ])
                filterrecord = end.next()
                count += 1
        except StopIteration, si:
            print "keeped ngrams = ", count
        excluded = Writer("basecsv://100125-fetopen-excluded.csv")
        excluded.writeRow(['id','label','pos_tag'])
        count = 0
        total=0
        selectgenerator = self.storage.select('NGram')
        try:
            record = selectgenerator.next()
            while record:
                total += 1
                if record[1]['id'] not in ngrams:
                    tag = self.reprpostag( record[1]['postag'] )
                    excluded.writeRow([ record[1]['id'], \
                        record[1]['label'], \
                        tag
                        ])
                    count += 1
                record = selectgenerator.next()
        except StopIteration, si:
            print "excluded ngrams = ", count
            print "total ngrams in db = ",total
        return


if __name__ == '__main__':
    program = Program()
    program.exportAll()
