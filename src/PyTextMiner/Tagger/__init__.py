# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Nov 19, 2009 6:32:44 PM$"

from nltk import pos_tag

class TreeBankPosTagger():
    """
     integration of nltk's standard POS tagger
     need nltk data 'taggers/maxent_treebank_pos_tagger/english.pickle'
    """

    @staticmethod
    def posTag( tokens ):
        return pos_tag( tokens )
