# -*- coding: utf-8 -*-

# time
from datetime import datetime
import unicode

class Document:
    """a Document containing targets"""
    def __init__(
            self,
            rawContent=None,
            title=None,
            date=None,
            targets=[]):
        """Document constructor.
        arguments: corpus, content, title, date, targets"""
        self.title = title
        if date:
            self.date = datetime.strptime(date, "%Y-%m-%d")
        else:
            self.date = datetime.today()
        self.targets = targets
        self.rawContent = rawContent

    def __str__(self):
        return self.title
    def __repr__(self):
        return "<%s>"%self.title
    def pushTarget(self):
        return
    def getTarget(self, targetID):
        return
