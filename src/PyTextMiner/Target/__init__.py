# -*- coding: utf-8 -*-
# PyTextMiner Parser
from .. import Parser

class Target:
    """Target containing ngrams"""

    def __init__(self,
        rawTarget,
        type=None,
        ngrams=[],
        minSize=1,
        maxSize=3,
        forbiddenChars='[^a-zA-Z\-\s\@ÀÁÂÆÄÇÈÉÊËÌÍÎÏÛÜÙÚ' \
                       'àáâãäåæçèéêëìíîïĨĩòóôõöÒÓÔÕÖÑùúûü]',
        separator = " "):

        """Text Target constructor"""
        self.type = type
        self.rawTarget = rawTarget
        self.sanitizedTarget = None
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize
        self.forbiddenChars = forbiddenChars
        self.separator = separator

        self.sanitizedTarget = self.sanitize(self.rawTarget)
        self.ngrams = self.tokenize(self.sanitizedTarget)

    def sanitize(self, text):
        """simple wrapper around Parser"""
        return Parser.Parser().sanitize(text, self.separator, self.forbiddenChars)

    def tokenize(self, text):
        """wrapper around Parser"""
        if self.maxSize >= 1 and self.maxSize >= self.minSize:
            return Parser.Parser().tokenize(text, self.minSize, self.maxSize, self.separator)

    def __repr__(self):
        return self.sanitizedTarget

