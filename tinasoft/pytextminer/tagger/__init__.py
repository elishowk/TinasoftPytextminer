#  Copyright (C) 2010 elishowk
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.


__author__="Elias Showk"
__date__ ="$Nov 19, 2009$"

# warning : nltk imports it's own copy of pyyaml
import nltk.corpus
import nltk.tag
import itertools
from nltk.tag import brill
from nltk import pos_tag

import logging
_logger = logging.getLogger('TinaAppLogger')

class TreeBankPosTagger():
    """
    integration of nltk Part Of Speech taggers
    static posTag method implemets default MaxentClassifier algorithm tagger
        needs nltk data 'taggers/maxent_treebank_pos_tagger/english.pickle'
    otherwise, instance of this class provides a self trained and composite tagger
        thanks to http://streamhacker.com/2010/04/12/pos-tag-nltk-brill-classifier/
        needs nltk data 'corpus/brown' and 'corpus/conll2000'
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

    training_corpus_size = 8000

    def __init__(self, training_corpus_size=None, trained_pickle=None ):
        # get the training sentences
        if training_corpus_size is not None:
            self.training_corpus_size = training_corpus_size
        _logger.debug( "Training tagger with training_corpus_size = %d"%self.training_corpus_size )
        brown_train = list(nltk.corpus.brown.tagged_sents()[:self.training_corpus_size])
        conll_train = list(nltk.corpus.conll2000.tagged_sents()[:self.training_corpus_size])
        train_sents = list(itertools.chain( brown_train, conll_train ))
        # base tagger classes for initial tagger
        tagger_classes = [nltk.tag.AffixTagger, nltk.tag.UnigramTagger, nltk.tag.BigramTagger, nltk.tag.TrigramTagger]

        backoff = nltk.tag.RegexpTagger(self.word_patterns)
        self.tagger = self._backoff(train_sents, tagger_classes, backoff=backoff)



    def _backoff(self, tagged_sents, tagger_classes, backoff=None):
        """Init backoff classes, takes a lot of time to train..."""
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

    def _normalize(self,tagtok):
        """returns an empty string when no tag was found"""
        tagtok = list(tagtok)
        if tagtok[1] is None:
            tagtok[1] = '?'
            #_logger.debug(tagtok)
        return tagtok

    def tag(self, tokens):
        """returns the tagged sentence using trained self.tagger"""
        tag_tokens = map(
            self._normalize,
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
