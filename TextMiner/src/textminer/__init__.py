# -*- coding: utf-8 -*-


__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:30:11 PM$"
"""TextMiner Module"""

# time
from time import gmtime, mktime


# PyTextMiner algorithms
import algorithms

class TextMiner:
    """TextMiner"""
    def __init__(self):
        pass

class Corpus:
    """a Corpus of Documents"""
    def __init__(self, name, documents=[]):
        self.name = name
        self.documents = documents
        
class Document:
    """a single Document"""
    def __init__(self, corpus, content="", title="", timestamp=mktime(gmtime()), targets=[]):
        """ Document constructor.
        arguments: corpus, content, title, timestamp, targets"""
        self.corpus = corpus
        self.title = title
        self.timestamp = timestamp
        self.targets = targets

class NGramExtractor:
    def __init__(self):
        pass

class Target:
    """a text Target in a Document"""
    def __init__(self, target, type=None, ngrams=[], minSize=1, maxSize=3):
        """Text Target constructor"""
        self.type = type
        self.target = target
        self.sanitizedTarget = None
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize

    def _sanitize(self, text):
        """simple wrapper around algorithms"""
        return algorithms.sanitize(text)

    def _ngrammize(self, text):
        """wrapper around algorithms"""
        if self.maxSize >= 1 and self.maxSize >= self.minSize:
            # get the ngrams
            i=0
            results = algorithms.ngrams(text,self.maxSize)
            for ngrams in results:
                i+=1
                print "%s-grams:"%i
                for ngram in ngrams:
                    print ngram
            return results

    def run(self):
        """Run the workflow"""
        # FAKE WORKFLOW
        step0 = self.target
        print "step0: raw data\n", step0
        step1 = self._sanitize(step0)
        print "step1: sanitize\n", step1
        step2 = self._ngrammize(step1)
        print "step2: extract ngrams\n", step2


class NGram:
    """an ngram"""
    def __init__(self, ngram, occurences=0):
        self.occurences = occurences
        self.ngram = ngram
    def __len__(self):
        """ return the length of the ngram"""
        return len(self.ngram)

class Project:
    def __init__(self, name, workflow):
        self.name = name
        self.workflow = workflow
        self.dataset = None

