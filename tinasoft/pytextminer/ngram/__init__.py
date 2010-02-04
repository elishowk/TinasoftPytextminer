# -*- coding: utf-8 -*-
# PyTextMiner NGram class
__author__="Elias Showk"

from tinasoft.pytextminer import PyTextMiner

import logging
_logger = logging.getLogger('TinaAppLogger')

class NGram(PyTextMiner):
    """NGram class"""
    def __init__(self, content, id=None, label=None, **metas):
        # normalize
        content = self.normalize(content)
        if label is None:
            label = " ".join(content)
        PyTextMiner.__init__(self, content, id, label, **metas)


class Filter():
    """
    Rule-based NGram content filtering
    applies on a generator of (ngram id, ngram obj)
    """
    rules={
        'any':[],
        'begin':['have','is','are'],
        'end':[],
        'both':[],
    }
    def __init__(self, config=None):
        """default rules based on english penn-treebank tagset"""
        self.lang='en'
        if config is not None:
            if rules in config:
                self.rules = config['rules']
            if lang in rules:
                self.lang = config['lang']

    def get_content(self, ng):
        if 'content' in ng[1]:
            return ng[1]['content']

    def _any(self, ng):
        contents = self.get_content(ng)
        if contents is not None:
            for content in contents:
                if content in self.rules['any']:
                    return False
        else:
            return True

    def any(self, nggenerator):
        """Given a NGram object generator, applies the _any() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._any(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _both(self, ng):
        contents = self.get_content(ng)
        if contents is not None:
            if contents[0] in self.rules['both'] or contents[-1] in self.rules['both']:
                return False
        else:
            return True

    def both(self, nggenerator):
        """Given a NGram object generator, applies the _both() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._both(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _begin(self, ng):
        contents = self.get_content(ng)
        if contents is not None:
            if contents[0] in self.rules['begin']:
                return False
        else:
            return True


    def begin(self, nggenerator):
        """Given a NGram object generator, applies the _begin() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._begin(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def _end(self, ng):
        contents = self.get_content(ng)
        if contents is not None:
            if contents[-1] in self.rules['end']:
                return False
        else:
            return True

    def end(self, nggenerator):
        """Given a NGram object generator, applies the _end() filter"""
        try:
            record = nggenerator.next()
            while record:
                if self._end(record) is not False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

    def all(self, ng):
        """Given an NGram object return True if ALL the tests passed"""
        return self._any(ng) and self._both(ng) and self._begin(ng) and self._end(ng)

class PosTagFilter(Filter):
    """
    Rule-based POS tag filtering
    applies on a generator of (ngram id, ngram obj)
    """

    rules = {
        'any':['PUN','SENT'],
        'begin':['POS'],
        'end':[],
        'both':['DT','CC','TO','IN','WDT','WP','WRB','PRP','EX','MD','UH'],
    }

    def get_content(self, ng):
        """overwrites the field selection to filter NGram object's postag"""
        if 'postag' in ng[1]:
            return [token[1] for token in ng[1]['postag']]

# WARNING : OBSOLETE !!!
#class NGramHelpers():
#    """Obsolete"""
#    @staticmethod
#    def filterUnique( rawDict, threshold, corpusNum, sqliteEncode ):
#        delList = []
#        filteredDict = {}
#        assocNGramCorpus = []
#        for ngid in rawDict.keys():
#            if rawDict[ ngid ].occs < threshold:
#                del rawDict[ ngid ]
#                delList.append( ngid )
#            else:
#                assocNGramCorpus.append( ( ngid, corpusNum, rawDict[ ngid ].occs ) )
#                item = rawDict[ ngid ]
#                filteredDict[ ngid ] = sqliteEncode(item)
#        return ( filteredDict, delList, assocNGramCorpus )

