# -*- coding: utf-8 -*-
from PyTextMiner.NGram import NGram

class Corpus:
    """a Corpus containing documents"""
    def __init__(self, name, documents=None, number=None):
        self.name = name
        if documents is None: 
            documents = []
        self.documents = documents
        self.number = number
        self.ngramDocFreqTable = None

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name

    def ngramDocFreq(self, targetType):
        self.ngramDocFreqTable = {}
        for doc in self.documents:
            for target in doc.targets:
                if target.type == targetType:
                    for ng in target.ngrams.itervalues():
                        newngram = NGram(
                            ngram = ng.ngram,
                            strRepr = ng.strRepr,
                            origin = ng.origin,
                            occs = 1
                        )
                        if self.ngramDocFreqTable.has_key( newngram.id ):
                            self.ngramDocFreqTable[ ng.id ].occs += 1
                        else:
                            self.ngramDocFreqTable[ newngram.id ] = newngram

        return self.ngramDocFreqTable


