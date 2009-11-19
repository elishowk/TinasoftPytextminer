# -*- coding: utf-8 -*-
# PyTextMiner Target class

class Target(dict):
    """Target containing ngrams"""

    def __init__(self,
        rawTarget,
        tokens=None,
        type=None,
        ngrams=None,
        ngramMin=1,
        ngramMax=2,
        forbChars=u"[^a-zA-Z0-9\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
        ngramSep= u"[\s]+",
        ngramEmpty = " ",
        #**opt
        ):
        """Text Target constructor"""
        if tokens is None:
            tokens = []
        # contains ngrams' generated unique IDs
        if ngrams is None:
            ngrams=set([])
        dict.__init__(self, rawTarget=rawTarget, tokens=tokens, ngrams=ngrams, forbChars=forbChars, ngramMin=ngramMin, ngramMax=ngramMax, ngramSep=ngramSep, ngramEmpty=ngramEmpty)

#    def __str__(self):
#        return self.rawTarget.encode('utf-8')
#    def __repr__(self):
#        return self.rawTarget.encode('utf-8')
