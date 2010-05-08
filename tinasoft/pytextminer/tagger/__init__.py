# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Nov 19, 2009 6:32:44 PM$"

# warning : nltk imports it's own copy of pyyaml
import nltk.corpus, nltk.tag, itertools
from nltk.tag import brill
from nltk import pos_tag
import logging
_logger = logging.getLogger('TinaAppLogger')

class TreeBankPosTagger():
    """
     integration of nltk's standard POS tagger
     need nltk data 'taggers/maxent_treebank_pos_tagger/english.pickle'
    """

    word_patterns = [
        (r'^-?[0-9]+(.[0-9]+)?$', 'CD'),
        (r'.*ould$', 'MD'),
        (r'.*ing$', 'VBG'),
        (r'.*ed$', 'VBD'),
        (r'.*ness$', 'NN'),
        (r'.*ment$', 'NN'),
        (r'.*ful$', 'JJ'),
        (r'.*ious$', 'JJ'),
        (r'.*ble$', 'JJ'),
        (r'.*ic$', 'JJ'),
        (r'.*ive$', 'JJ'),
        (r'.*ic$', 'JJ'),
        (r'.*est$', 'JJ'),
        (r'^a$', 'PREP'),
    ]


    def __init__(self):
        # set of training sentences
        #brown_review_sents = nltk.corpus.brown.tagged_sents(categories=['reviews'])
        #brown_lore_sents = nltk.corpus.brown.tagged_sents(categories=['lore'])
        #brown_romance_sents = nltk.corpus.brown.tagged_sents(categories=['romance'])

        brown_train = list(nltk.corpus.brown.tagged_sents()[:10000])
        conll_train = list(nltk.corpus.conll2000.tagged_sents()[:10000])
        train_sents = list(itertools.chain( brown_train, conll_train ))
        # base tagger classes for initial tagger
        tagger_classes = [nltk.tag.AffixTagger, nltk.tag.UnigramTagger, nltk.tag.BigramTagger, nltk.tag.TrigramTagger]

        backoff = nltk.tag.RegexpTagger(self.word_patterns)
        #aubtr_tagger = nltk.tag.RegexpTagger(word_patterns, backoff=aubt_tagger)
        self.tagger = self.backoff(train_sents, tagger_classes, backoff=backoff)

        #templates = [
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateTagsRule, (1,1)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateTagsRule, (2,2)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateTagsRule, (1,2)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateTagsRule, (1,3)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateWordsRule, (1,1)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateWordsRule, (2,2)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateWordsRule, (1,2)),
        #    brill.SymmetricProximateTokensTemplate(brill.ProximateWordsRule, (1,3)),
        #    brill.ProximateTokensTemplate(brill.ProximateTagsRule, (-1, -1), (1,1)),
        #    brill.ProximateTokensTemplate(brill.ProximateWordsRule, (-1, -1), (1,1))
        #]

        #trainer = brill.FastBrillTaggerTrainer(backoff_tagger, templates)
        #self.tagger = trainer.train(brown_train, max_rules=100, min_score=3)


    def backoff(self, tagged_sents, tagger_classes, backoff=None):
        if not backoff:
            backoff = tagger_classes[0](tagged_sents)
            del tagger_classes[0]

        for cls in tagger_classes:
            tagger = cls(tagged_sents, backoff=backoff)
            backoff = tagger

        return backoff

    @staticmethod
    def posTag( tokens ):
        """each tokens becomes ['token','TAG']"""
        return map( list, pos_tag( tokens ) )

    def normalize(self,tagtok):
        tagtok = list(tagtok)
        if tagtok[1] is None:
            tagtok[1] = ''
            #_logger.debug(tagtok)
        return tagtok

    def tag(self, tokens):
        tag_tokens = map(
            self.normalize,
            self.tagger.tag( tokens )
        )
        return tag_tokens

    @staticmethod
    def getContent( sentence ):
        """return words from a tagged list"""
        return [tagged[0] for tagged in sentence]

    @staticmethod
    def getTag( sentence ):
        """return TAGS from a tagged list"""
        return [tagged[1] for tagged in sentence]
