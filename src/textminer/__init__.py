# -*- coding: utf-8 -*-


__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:30:11 PM$"

"""TextMiner Module"""

# time
from time import gmtime, mktime

# PyTextMiner algorithms
import algorithms

from shove import Shove
root = Shove() # default file-based shove

class TextMiner:
    """TextMiner"""
    def __init__(self):
        pass

class Document:
    """a single Document"""
    def __init__(self, corpus, content="", title="",
                       timestamp=mktime(gmtime()), targets=[]):
        """ Document constructor.
        arguments: corpus, content, title, timestamp, targets"""
        self.corpus = corpus
        self.title = title
        self.timestamp = timestamp
        self.targets = targets
        self.content = content

    def __str__(self):
        return self.title

    def __repr__(self):
        return "<%s>"%self.title

class Corpus:
    """a Corpus of Documents"""
    def __init__(self, name, documents=[]):
        self.name = algorithms.charsertnormalize(name)
        self.documents = documents


    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s>"%self.name


class Target:
    """a text Target in a Document"""
    def __init__(self, target, type=None, ngrams=[], minSize=1, maxSize=3, forbidenChars='[^a-zA-Z\-\s\@ÀÁÂÆÄÇÈÉÊËÌÍÎÏÛÜÙÚàáâãäåæçèéêëìíîïĨĩòóôõöÒÓÔÕÖÑùúûü]', separator = " "):
        """Text Target constructor"""
        self.type = type
        self.target = target
        self.sanitizedTarget = None
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize
	self.forbidenChars = forbidenChars
	self.separator = separator

    def _sanitize(self, text):
        """simple wrapper around algorithms"""
        return algorithms.sanitize(text, self.separator, self.forbidenChars)

    def _ngrammize(self, text):
        """wrapper around algorithms"""
        if self.maxSize >= 1 and self.maxSize >= self.minSize:
            return algorithms.tokenize(text, self.minSize, self.maxSize, self.separator)

    def run(self):
        """Run the workflow"""
        self.sanitizedTarget = self._sanitize(self.target)
        self.ngrams = self._ngrammize(self.sanitizedTarget)


class NGram:
    """an ngram"""
    def __init__(self, ngram, occurences=0):
        self.occurences = occurences
        self.ngram = ngram
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)
