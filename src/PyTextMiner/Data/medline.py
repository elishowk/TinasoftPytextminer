# -*- coding: utf-8 -*-

from datetime import datetime
import codecs

import PyTextMiner
from  PyTextMiner import Target, Document, Corpus

from PyTextMiner import Data
        
class Model (object):
    def __init__(self, lines):
        binds = { 
            "TI"  : ("title",str),
            "AB"  : ("abstract",str),
            "AU"  : ("author",str),
            "FAU" : ("fullname",str),
            "JT"  : ("pubname",str),
            "DP"  : ("pubdate",datetime)
        }
        buff = ""
        for line in lines:
            prefix = line[:4].strip().upper()
            raw =  line[6:]

            if len(buff) > 0 and prefix == "":
                buff = "%s%s" % (buff,raw)

            elif prefix == "AB":
                buff = "%s"%raw
            else:

                # complete the abstract buffer and add it as attribute
                if len(buff) > 0:
                    content = "%s" % buff
		    attribute, type = binds["AB"]
		    self.__setattr__(attribute, type(content))
                    buff = ""

                # add the attribute
                content = "%s" % raw
                try:
		    attribute, type = binds[prefix]
		    self.__setattr__(attribute, type(content))
		except Exception, exc:
		    pass


class Importer (Data.Importer):
    def __init__(self, path, locale='en_US:UTF-8'):
        self.locale =  locale
        self.lang,self.encoding = self.locale.split(':')
        file = codecs.open(path, "rU", self.encoding)
        self.documents = Importer._load_documents(file, self.locale)
        self.corpus = Corpus( name=path, documents=self.documents )

    @staticmethod
    def _load_documents(file, locale):
        docs = []
        lines = []
        for line in file.readlines():
            line = line.rstrip()
            if line != "":
                lines.append( line )
                continue

            model = Model(lines)
            try:
                concat = "%s"%model.title
                concat += "%s"%model.abstract
            except:
                pass

            docs += [ Document(
                rawContent=model,
                title=model.title,
                targets=[ Target(
                     rawTarget=concat,
                     type='testType',
                     locale=locale.replace(":","."))])]
            lines = []
        return docs
        
