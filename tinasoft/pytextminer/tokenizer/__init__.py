# -*- coding: utf-8 -*-
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

__author__ = "Elias Showk"
__date__ = "$Oct 20, 2009 6:32:44 PM$"

import nltk

import logging
import re
import string
from tinasoft.pytextminer import ngram
from tinasoft.pytextminer import tagger
from tinasoft.pytextminer import filtering
_logger = logging.getLogger('TinaAppLogger')

class RegexpTokenizer():
    """
    A homemade tokenizer that splits a text into tokens
    given a regexp used as a separator
    """
    @staticmethod
    def sanitize(input, forbiddenChars, emptyString):
        """sanitized a text

        @return str: text
        """
        striped = string.strip(input)
        #replaces forbidden characters by a separator
        sanitized = re.sub(forbiddenChars, emptyString, striped)
        return sanitized.lower()

    @staticmethod
    def cleanPunct(text, emptyString, punct=u'[\,\.\;\:\!\?\"\[\]\{\}\(\)\<\>]'):
        #print text
        noPunct = re.sub(punct, emptyString, text)
        return noPunct

    ### deprecated
    @staticmethod
    def tokenize(text, separator, emptyString, stopwords=None):
        noPunct = RegexpTokenizer.cleanPunct(text, emptyString)
        tokens = re.split(separator, noPunct)
        return tokens

    @staticmethod
    def ngramize(minSize, maxSize, tokens, emptyString, stopwords=None, filters=[]):
        """
            returns a dict of NGram instances
            using the optional stopwords object to filter by ngram length
            tokens = [sentence1, sentence2, etc]
            sentences = list of words = [word,postagged_word]
        """
        ngrams = {}
        for sent in tokens:
            content = tagger.TreeBankPosTagger.getContent(sent)
            for i in range(len(content)):
                for n in range(minSize, maxSize + 1):
                    if len(content) >= i + n:
                        #content = tagger.TreeBankPosTagger.getContent(sent[i:n+i])
                        ng = ngram.NGram(content[i:n + i], occs=1, postag=sent[i:n + i])
                        if ng['id'] in ngrams:
                            # exists in document : only increments occs
                            ngrams[ng['id']]['occs'] += 1
                        else:
                            if stopwords is None or stopwords.contains(ng) is False:
                                if filtering.apply_filters(ng, filters) is True:
                                    ngrams[ng['id']] = ng
        return ngrams

class TreeBankWordTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then cleans the punctuation
    before tokenizing using nltk.TreebankWordTokenizer()
    """
    @staticmethod
    def tokenize(text, emptyString, stopwords=None):
        sentences = nltk.sent_tokenize(text)
        # WARNING : only works on english
        sentences = [nltk.TreebankWordTokenizer().\
            tokenize(RegexpTokenizer.cleanPunct(sent, emptyString)) \
            for sent in sentences]
        return sentences

    @staticmethod
    def extract(doc, stopwords, ngramMin, ngramMax, filters, tagger):
        sanitizedTarget = TreeBankWordTokenizer.sanitize(
                                                    doc['content'],
                                                    doc['forbChars'],
                                                    doc['ngramEmpty']
                                                        )
        sentenceTokens = TreeBankWordTokenizer.tokenize(
                                                    text=sanitizedTarget,
                                                    emptyString=doc['ngramEmpty'],
                                                    )
        tokens = []
        for sentence in sentenceTokens:
            tokens.append(tagger.tag(sentence))
        return TreeBankWordTokenizer.ngramize(
                                        minSize=ngramMin,
                                        maxSize=ngramMax,
                                        tokens=tokens,
                                        emptyString=doc['ngramEmpty'],
                                        stopwords=stopwords,
                                        filters=filters,
                                            )
