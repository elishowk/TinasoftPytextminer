# -*- coding: utf-8 -*-
# PyTextMiner NGram static helpers class

__author__="Elias Showk"

class NGram():
    """ngram static method helpers"""

    @staticmethod
    def normalize( ng ):
        def normalizePOS(lst):
            return lst[0].lower()
        return map( normalizePOS, ng )

    @staticmethod
    def generateID( lst ):
        return abs(hash("".join( lst )))

    @staticmethod
    def filterUnique( rawDict, threshold ):
        for ngid in rawDict.keys():
            if rawDict[ ngid ]['occs'] < threshold:
                del rawDict[ ngid ]
        return rawDict

