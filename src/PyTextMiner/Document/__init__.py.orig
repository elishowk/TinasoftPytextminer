# -*- coding: utf-8 -*-

# core modules
from datetime import datetime

class Document:
    """a Document containing targets"""
    def __init__(
            self,
            rawContent,
            title=None,
            date=None,
            targets=None,
            author=None):
            
        """Document constructor.
        arguments: corpus, content, title, date, targets"""
        
        self.rawContent = rawContent
        self.title = title
        
        if date is None:
            self.date = datetime.today()
        else:
            self.date = datetime.strptime(date, "%Y-%m-%d")
            
        if targets is None: 
            targets = []
            
        self.targets = targets
        self.author = author

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
