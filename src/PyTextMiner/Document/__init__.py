# -*- coding: utf-8 -*-
from datetime import datetime

class Document():
    """a Document containing targets"""
    def __init__(
            self,
            rawContent,
            title=None,
            date=None,
            targets=[]):
        """Document constructor.
        arguments: corpus, content, title, date, targets"""
        self.title = title
        #self.date = date
        if date:
            self.date = datetime.strptime(date, "%Y-%m-%d")
        else:
            self.date = datetime.today()
        self.targets = targets
        self.rawContent = rawContent

    def __str__(self):
        #return self.rawContent.encode('utf-8')
        return "%s"%self.rawContent

    def __repr__(self):
        #return "<%s>"%self.rawContent.encode('utf-8')
        return "<%s>"%self.rawContent

    def pushTarget(self):
        return
    def getTarget(self, targetID):
        return
