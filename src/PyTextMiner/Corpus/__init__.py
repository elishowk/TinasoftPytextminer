# -*- coding: utf-8 -*-
class Corpus():
    """a Corpus containing documents"""
    def __init__(self, name, documents=[]):
        self.name = name
        self.documents = documents

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name

    #def parseDocs(self):
    #    return
    #def getDoc(self, docID):
    #    return
