# -*- coding: utf-8 -*-

class Corpus:
    """a Corpus containing documents"""
    def __init__(self, name=None, documents=None):
        # contains documents' generated unique IDs
        if documents is None: 
            documents = set([])
        self.documents = documents
        self.name = name

#    def __str__(self):
#        return self.name
#
#    def __repr__(self):
#        return "<%s>"%self.name

    #def ngramDocFreq(self, targetType):
    #    self.ngramOccPerDoc = {}
    #    for doc in self.documents:
    #        for target in doc.targets:
    #            if target.type == targetType:
    #                for ng in target.ngrams.itervalues():
    #                    newngram = {
    #                            ngram : ng.ngram,
    #                            strRepr : ng.strRepr,
    #                            origin : ng.origin,
    #                            occs : 1
    #                            }
    #                    if self.ngramOccPerDoc.has_key( newngram.id ):
    #                        self.ngramOccPerDoc[ ng.id ].occs += 1
    #                    else:
    #                        self.ngramOccPerDoc[ newngram.id ] = newngram
    #return self.ngramOccPerDoc


