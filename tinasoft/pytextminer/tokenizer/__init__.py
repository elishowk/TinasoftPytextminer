#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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

__author__="elishowk@nonutc.fr"

import nltk
import re
import string

from tinasoft.pytextminer import ngram
from tinasoft.pytextminer import tagger
from tinasoft.pytextminer import filtering
from tinasoft.pytextminer import PyTextMiner

import logging
_logger = logging.getLogger('TinaAppLogger')

nltk_treebank_tokenizer = nltk.TreebankWordTokenizer()

# We consider following rules to apply whatever be the langage.
# ... is an ellipsis, put spaces around before splitting on spaces
# (make it a token)
ellipfind_re = re.compile(ur"(\.\.\.)",
                          re.IGNORECASE|re.VERBOSE)
ellipfind_subst = ur" ... "
# A regexp to put spaces if missing after alone marks.
punct1find_re = re.compile(ur"(["+string.punctuation+"])([^ ])",
                           re.IGNORECASE|re.VERBOSE)
punct1find_subst = ur"\1 \2"
# A regexp to put spaces if missing before alone marks.
punct2find_re = re.compile(ur"([^ ])([["+string.punctuation+"])",
                           re.IGNORECASE|re.VERBOSE)
punct2find_subst = ur"\1 \2"

class RegexpTokenizer():
    """
    A faster homemade tokenizer that splits a text into tokens
    given a regexp used as a separator
    """
    @staticmethod
    def sanitize(input, forbiddenChars, emptyString):
        """
        basic @input text sanitizing
        @return str: text
        """
        striped = string.strip(input)
        #replaces forbidden characters by a separator
        sanitized = re.sub(forbiddenChars, emptyString, striped)
        return sanitized

    @staticmethod
    def cleanPunct(text, emptyString=" ", punct=u'[\,\.\;\:\!\?\"\[\]\{\}\(\)\<\>]'):
        """
        old school regexp based text cleaner
        """
        noPunct = re.sub(punct, emptyString, text)
        return noPunct

    @staticmethod
    def tokenize(text, separator="\s+", emptyString=" "):
        """
        old school regexp based tokenizer
        """
        noPunct = RegexpTokenizer.cleanPunct(text, emptyString)
        tokens = re.split(separator, noPunct)
        return tokens

    @staticmethod
    def ngramize(minSize, maxSize, content):
        """
        simple ngramizing method
        returns a list of ngrams (as words lists) ordered by ngram length
        """
        ngrams = []
        for size in range(maxSize):
            ngrams.append([])
        for i in range(len(content)):
            for n in range(minSize, maxSize + 1):
                if len(content) >= i + n:
                    ngrams[n-1].append(content[i:n + i])
        return ngrams

class TreeBankWordTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then cleans the punctuation
    before tokenizing using nltk.TreebankWordTokenizer()
    """
    @staticmethod
    def sanitize(input):
        """
        basic @input text sanitizing
        @return str: text
        """
        # Put blanks before and after '...' (extract ellipsis).
        # Put space between punctuation ;!?:, and following text if space missing.
        # Put space between text and punctuation ;!?:, if space missing.
        punct2find_re.sub(
            punct2find_subst,
            punct1find_re.sub(
                punct1find_subst,
                ellipfind_re.sub(
                    ellipfind_subst,
                    input
                )
            )
        )
        return string.strip(input)

    @staticmethod
    def tokenize(text, tagger):
        """
        Cuts a @text in sentences of tagged tokens
        using nltk Treebank tokenizer
        and a @tagger object
        """
        sentences = nltk.sent_tokenize(text)
        for sent in sentences:
            yield tagger.tag(nltk_treebank_tokenizer.tokenize(sent))

    @staticmethod
    def extract(doc, stopwords, ngramMin, ngramMax, filters, tagger, stemmer):
        """
        sanitizes content and label texts
        tokenizes it
        POS tags the tokens
        constructs the resulting NGram objects
        """
        sentenceTaggedTokens = TreeBankWordTokenizer.tokenize(
            TreeBankWordTokenizer.sanitize(
                doc['content'] +" . "+ doc['label']
            ),
            tagger
        )
        try:
            while 1:
                nextsent = sentenceTaggedTokens.next()
                # updates ngrams
                ngrams = TreeBankWordTokenizer.ngramize(
                    minSize = ngramMin,
                    maxSize = ngramMax,
                    tagTokens = nextsent,
                    stopwords = stopwords,
                    filters = filters,
                    stemmer = stemmer
                )
        except StopIteration, stopit:
            return ngrams

    @staticmethod
    def ngramize(minSize, maxSize, tagTokens, stopwords, filters, stemmer):
        """
            common ngramizing method
            returns a dict of NGram instances
            using the optional stopwords object to filter by ngram length
            tokens = [[sentence1 tokens], [sentence2 tokens], etc]
            sentences = list of tuples = [(word,TAG_word), etc]
        """
        # content is the list of words from tagTokens
        ngrams = {}
        content = tagger.TreeBankPosTagger.getContent(tagTokens)
        stemmedcontent = []
        for word in content:
             stemmedcontent += [stemmer.stem(word)]
        # tags is the list of tags from tagTokens
        tags = tagger.TreeBankPosTagger.getTag(tagTokens)
        for i in range(len(content)):
            for n in range(minSize, maxSize + 1):
                if len(content) >= i + n:
                    # new NGram instance only if it doesn't exist
                    id = ngram.NGram.getNormId(stemmedcontent[i:n+i])
                    if id in ngrams:
                        label = PyTextMiner.form_label( content[i:n+i] )
                        postag = PyTextMiner.form_label( tags[i:n+i] )
                        edges = { 'label': { label: 1 }, 'postag': { postag: 1 } }
                        # already exists in document : increments occs and updates edges
                        ngrams[id]['occs'] += 1
                        ngrams[id] = PyTextMiner.updateEdges( edges, ngrams[id] )
                    else:
                        ng = ngram.NGram(
                            stemmedcontent[i:n+i],
                            id = id,
                            label=PyTextMiner.form_label(content[i:n+i]),
                            occs=1,
                            postag=tags[i:n + i]
                        )
                        # first stopwords filters, then content and postag filtering
                        if stopwords is None or stopwords.contains(ng) is False:
                            if filtering.apply_filters(ng, filters) is True:
                                ngrams[id] = ng
        return ngrams
