# -*- coding: utf-8 -*-
__author__="Elias Showk"

import datetime
from tinasoft.pytextminer import PyTextMiner

class Document(PyTextMiner):
    """a Document containing targets containing ngrams"""

    def __init__(
            self,
            content,
            docNum,
            title,
            datestamp=None,
            author=None,
            targets=[],
            tokens=[],
            ngrams=[],
            ngramMin=1,
            ngramMax=3,
            forbChars="[^a-zA-Z\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
            ngramSep= u"[\s]+",
            ngramEmpty = " ",
            **metas
        ):
        PyTextMiner.__init__(self, content, docNum, title, **metas)
        self.date = datestamp
        self.author = author
        # targets is a list of texts (incrementally) sanitized
        self.targets = targets
        # tokens are words
        self.tokens = tokens
        # ngrams is a list of ngram's IDs
        self.ngrams = ngrams
        # these are tokenization paramaters
        self.ngramMin = ngramMin
        self.ngramMax = ngramMax
        self.forbChars = forbChars
        self.ngramSep = ngramSep
        self.ngramEmpty = ngramEmpty

    def getDate(self):
        if self.date is not None:
            try:
                return datetime.strptime(string,"%Y%m%d").date()
            except Exception, e:
                # log exception
                return None
