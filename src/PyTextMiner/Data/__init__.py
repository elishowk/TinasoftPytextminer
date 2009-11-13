# -*- coding: utf-8 -*-

from datetime import datetime

import PyTextMiner
from  PyTextMiner import Target, Document, Corpus

class Importer (object):
    def __init__(self):
        pass

class MedlineDict (dict):
    def __init__(self, lines):
        dict.__init__(self)
	binds = { 
            "TI"  : ("title",str),
            "AB"  : ("abstract",str),
            "AU"  : ("author",str),
            "FAU" : ("fullname",str),
            "JT"  : ("pubname",str),
            "DP"  : ("pubdate",datetime)
	}
        for line in self.lines:
            attribute, type = binds[line[:4].strip().upper()]
            self.__setattribute__(attribute, type(line[6:]))

class MedlineFile (Importer):
    def __init__(self, path, locale='en_US:utf-8'):
        self.documents = MedlineFile._load_documents(open(path, "r"), locale)
        self.corpus = Corpus( name=path, documents=self.documents)

    @staticmethod
    def _load_documents(file, locale):
        docs, lines = [], []
        for line in [line.strip() for line in file.readlines()]:
            if line != "":
                lines += line
                continue
            tmp = MedlineDict(lines)
            docs += [ Document(
                rawContent=tmp,
                title=tmp.title,
                targets=[ Target(
                     rawTarget="%s%s"%(tmp.title,tmp.abstract),
                     type='testType',
                     locale=locale)])]
            lines = []
        return docs

