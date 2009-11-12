# -*- coding: utf-8 -*-
class Corpus():
    """a Corpus containing documents"""
    def __init__(self, name, documents=None):
        self.name = name
        if documents is None: 
            documents = []
        self.documents = documents

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name
