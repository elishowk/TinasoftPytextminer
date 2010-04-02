# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Nov 19, 2009 6:32:44 PM$"

# warning : nltk imports it's own copy of pyyaml
import nltk
#nltk.data.path = ['shared/nltk_data']
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

    @staticmethod
    def getContent( sentence ):
        """return words from a tagged list"""
        return [tagged[0] for tagged in sentence]

    @staticmethod
    def getTag( sentence ):
        """return TAGS from a tagged list"""
        return [tagged[1] for tagged in sentence]
