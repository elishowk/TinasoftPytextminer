# -*- coding: utf-8 -*-
# PyTextMiner NGram
class Target():
    """Target containing ngrams"""

    def __init__(self,
        rawTarget,
        type=None,
        ngrams=[],
        minSize=1,
        maxSize=3,
        forbiddenChars=u"[^a-zA-Z\-\s\@ÀÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\']",
        separator=" ",
        sanitizedTarget=None
        ):

        """Text Target constructor"""
        self.rawTarget = rawTarget
        self.type = type
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize
        self.forbiddenChars = forbiddenChars
        self.separator = separator
        self.sanitizedTarget = sanitizedTarget
    def __str__(self):
        return self.rawTarget
    def __repr__(self):
        return self.rawTarget
