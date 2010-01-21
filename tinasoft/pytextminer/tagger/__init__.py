# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Nov 19, 2009 6:32:44 PM$"

# warning : nltk imports it's own copy of pyyaml
import nltk
nltk.data.path = ['shared/nltk_data']
from nltk import pos_tag

class TreeBankPosTagger():
    """
     integration of nltk's standard POS tagger
     need nltk data 'taggers/maxent_treebank_pos_tagger/english.pickle'
    """

    @staticmethod
    def posTag( tokens ):
        """each tokens becomes ['token','TAG']"""
        return map( list, pos_tag( tokens ) )

class PosFilter():
    """
    Rules bases POS tag filtering
    applies on a list of (ngram id, ngram obj)
    """
    def __init__(self, config=None):
        """default rules based on english penn-treebank tagset"""
        self.lang='en'
        self.rules={
            'any':['PUN','SENT'],
            'begin':['POS'],
            'end':[],
            'both':['DT','CC','TO','IN','WDT','WP','WRB','PRP','EX','MD','UH'],
        }
        if config is not None:
            if rules in config:
                self.rules = config['rules']
            if lang in rules:
                self.lang = config['lang']
        return

    def get_postag(self, ng):
        if 'postag' in ng[1]:
            return [token[1] for token in ng[1]['postag']]

    def _any(self, ng):
        for postag in self.get_postag(ng):
            if postag in self.rules['any']:
                return False

    def any(self, nggenerator):
        try:
            record = nggenerator.next()
            while record:
                if self._any(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _both(self, ng):
        postags = self.get_postag(ng)
        if postags[0] in self.rules['both'] or postags[-1] in self.rules['both']:
            return False

    def both(self, nggenerator):
        try:
            record = nggenerator.next()
            while record:
                if self._both(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _begin(self, ng):
        postags = self.get_postag(ng)
        if postags[0] in self.rules['begin']:
            return False

    def begin(self, nggenerator):
        try:
            record = nggenerator.next()
            while record:
                if self._begin(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _end(self, ng):
        postags = self.get_postag(ng)
        if postags[-1] in self.rules['end']:
            return False

    def end(self, nggenerator):
        try:
            record = nggenerator.next()
            while record:
                if self._end(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return
