# -*- coding: utf-8 -*-

class Corpus:
    """a Corpus containing documents"""
    def __init__(self, name, documents=None, number=None):
        self.name = name
        if documents is None: 
            documents = []
        self.documents = documents
        self.number = number

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name

    def ngramDocFreq(self, targetType):
        freqTable = set()
        for doc in self.documents:
            for target in doc.targets:
                if target.type == targetType:
                    for ng in target.ngrams:
                        if ng in freqTable:
                            freqTable[ ng ].occs += 1
                            print "OCCS ===", ng.occs
                        else :
                            ng.occs = 1
                            freqTable.add( ng )
        return freqTable


