# -*- coding: utf-8 -*-
__author__="Elias Showk"

import datetime
from tinasoft.pytextminer import PyTextMiner

class Document(PyTextMiner):
    """a Document containing targets containing ngrams"""

    def __init__(
            self,
            rawContent,
            datestamp=None,
            docNum=None,
            targets=None,
            tokens=None,
            ngrams=None,
            ngramMin=1,
            ngramMax=2,
            forbChars="[^a-zA-Z\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
            ngramSep= u"[\s]+",
            ngramEmpty = " ",
            **opt
        ):
        self.id = str(docNum)
        self.date = datestamp
        self.rawContent = rawContent
        # sanitized targets are a list of sanitzed texts
        if targets is None:
            targets = []
        self.targets = targets
        # tokens are words
        if tokens is None:
            tokens = []
        self.tokens = tokens
        # ngrams is a list of ngram's IDs
        if ngrams is None:
            ngrams = []
        self.ngrams = ngrams
        # these are tokenization paramaters
        self.ngramMin=ngramMin
        self.ngramMax=ngramMax
        self.forbChars=forbChars
        self.ngramSep= ngramSep
        self.ngramEmpty = ngramEmpty
        # optional document fields
        self.loadOptions( opt )
    
    def getDate(self):
        if self.date is not None:
            try:
                return datetime.strptime(string,"%Y%m%d").date()
            except Exception, e:
                # log exception
                return None
