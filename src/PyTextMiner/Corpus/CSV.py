# -*- coding: utf-8 -*-
from . import Corpus
from ..Document import Dict

import csv

class CSV(Corpus):
    """a CSV file Corpus"""
    def __init__(self, name, file, title, timestamp, delimiter=';', quotechar=None):
        Corpus.__init__(self, name=name)
        self.titleField = title
        self.timestampField = timestamp
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.csv = csv.reader(
                file,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = self.csv.next()

    def parseDocs(self):
        for doc in self.csv:
            #print doc[self.titleField]
            self.documents += [Dict.Dict(self.fieldNames, doc, doc[self.titleField], doc[self.timestampField])]
        
