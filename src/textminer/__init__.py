# -*- coding: utf-8 -*-
__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:30:11 PM$"

"""Corpora Module"""

# time
from time import gmtime, mktime

# PyTextMiner tokenizer
import tokenizer

class Corpora:
    """Corpora contains a list of a corpus"""
    def __init__(self, corpora=[]):
        self.corpora = corpora

class Corpus:
    """a Corpus containing documents"""
    def __init__(self, name, documents=[]):
        self.name = name
        self.documents = documents

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name

    def parseDocs(self):
        return
    def pushDoc(self):
        return
    def getDoc(self, docID):
        return

class Document:
    """a Document containing targets"""
    def __init__(self, rawContent, title="",
                       timestamp=mktime(gmtime()), targets=[]):
        """Document constructor.
        arguments: corpus, content, title, timestamp, targets"""
        self.title = title
        self.timestamp = timestamp
        self.targets = targets
        self.rawContent = rawContent

    def __str__(self):
        return self.title

    def __repr__(self):
        return "<%s>"%self.title
    def pushTarget(self):
        return
    def getTarget(self, targetID):
        return


class Target:
    """Target containing ngrams"""

    def __init__(self,
        rawTarget,
        type=None,
        ngrams=[],
        minSize=1,
        maxSize=3,
        forbiddenChars='[^a-zA-Z\-\s\@ÀÁÂÆÄÇÈÉÊËÌÍÎÏÛÜÙÚ' \
                       'àáâãäåæçèéêëìíîïĨĩòóôõöÒÓÔÕÖÑùúûü]',
        separator = " "):

        """Text Target constructor"""
        self.type = type
        self.rawTarget = rawTarget
        self.sanitizedTarget = None
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize
        self.forbiddenChars = forbiddenChars
        self.separator = separator

        self.sanitizedTarget = self.sanitize(self.rawTarget)
        self.ngrams = self.tokenize(self.sanitizedTarget)

    def sanitize(self, text):
        """simple wrapper around tokenizer"""
        return tokenizer.sanitize(text, self.separator, self.forbiddenChars)

    def tokenize(self, text):
        """wrapper around tokenizer"""
        if self.maxSize >= 1 and self.maxSize >= self.minSize:
            return tokenizer.tokenize(text, self.minSize, self.maxSize, self.separator)

    def __repr__(self):
        return self.sanitizedTarget

class NGram:
    """an ngram"""
    def __init__(self, ngram, occs=None):
        self.occs = occs
        self.ngram = ngram
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)
