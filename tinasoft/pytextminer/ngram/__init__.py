# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

import re

import logging
_logger = logging.getLogger('TinaAppLogger')

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, tokenlist, id=None, label=None, edges=None, **metas):
        # normalize
        tokenlist = self.normalize(tokenlist)
        if label is None:
            label = " ".join(tokenlist)
        if edges is None:
            edges = { 'Document' : {}, 'Corpus' : {} }
        PyTextMiner.__init__(self, tokenlist, id, label, edges, **metas)

    def addEdge(self, type, key, value):
        if type == 'Document':
            return self._addUniqueEdge( type, key, value )
        else:
            return self._addEdge( type, key, value )

class Filter():
    """
    Rule-based NGram content filtering
    applies on a generator of (ngram_objects)
    """
    # TODO move rules into app config, and add language support
    rules={
        'any':[''],
        'begin':[],
        'end':[],
        'both':['by','in','of','a','have','is','are','or','and'],
    }
    def __init__(self, config=None):
        """default rules based on english stopwords"""
        #self.lang='en'
        if config is not None:
            if rules in config:
                self.rules = config['rules']
            if lang in rules:
                self.lang = config['lang']

    def get_content(self, ng):
        if 'content' in ng:
            return ng['content']

    def _any(self, ng):
        contents = self.get_content(ng)
        test = True
        if contents is not None:
            for content in contents:
                if content in self.rules['any']:
                    test = False
        return test

    def any(self, nggenerator):
        """NGram generator, applies the _any() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._any(record) is True:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _both(self, ng):
        contents = self.get_content(ng)
        test = True
        if contents is not None:
            if contents[0] in self.rules['both'] or contents[-1] in self.rules['both']:
                test = False
        return test

    def both(self, nggenerator):
        """NGram generator, applies the _both() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._both(record) is True:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _begin(self, ng):
        contents = self.get_content(ng)
        test = True
        if contents is not None:
            if contents[0] in self.rules['begin']:
                test = False
        return test

    def begin(self, nggenerator):
        """NGram generator, applies the _begin() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._begin(record) is True:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _end(self, ng):
        contents = self.get_content(ng)
        test = True
        if contents is not None:
            if contents[-1] in self.rules['end']:
                test = False
        return test

    def end(self, nggenerator):
        """NGram generator, applies the _end() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._end(record) is True:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def test(self, ng):
        """returns True if ALL the tests passed"""
        return self._any(ng) and self._both(ng) and self._begin(ng) and self._end(ng)

class PosTagFilter(Filter):
    """
    Rule-based POS tag filtering
    """
    # TODO move rules into app config, and add language support
    rules = {
        'any':['PUN','SENT'],
        'begin':['POS'],
        'end':[],
        'both':['VB','VDB','VBP','VBZ','DT','CC','TO','IN','WDT','WP',\
            'WRB','PRP','EX','MD','UH'],
    }

    def get_content(self, ng):
        """selects NGram's postag"""
        return [token[1] for token in ng['postag']]

class PosTagValid(PosTagFilter):
    """
    Regexp-based POS tag filteringvalidation

    """
    # TODO move rules into app config, and add language support
    rules = {
        'standard': re.compile(r"^((VB.?,|JJ.?,){0,2}?NN.?,)+?((IN.?,|CC.?,)((VB.?,|JJ.?,){0,2}?NN.?,)+?)*?$")
    }

    def standard(self, nggenerator):
        """NGram generator, applies the _standard() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._standard(record[1]) is True:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _standard(self, ng):
        content = self.get_content(ng)
        pattern = ",".join(content)
        pattern += ","
        if self.rules['standard'].match( pattern ) is None:
            #rint "REFUSE %s : %s"%(ng['label'],pattern)
            return False
        else:
            #print "ACCEPT %s : %s"%(ng['label'],pattern)
            return True

    def test(self, ng):
        """returns True if ALL the tests passed"""
        return self._standard(ng)
