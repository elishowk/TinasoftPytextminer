# -*- coding: utf-8 -*-
from tinasoft.data import Exporter, Importer
from datetime import datetime
import codecs

#from tinasoft.pytextminer import *
        
class Model (object):
    def __init__(self, lines):
        binds = { 
            "TI"  : ("title", str, ""),
            "AB"  : ("abstract", str, ""),
            "AU"  : ("author", str, ""),
            "FAU" : ("fullname", str, ""),
            "JT"  : ("pubname", str, ""),
            "DP"  : ("pubdate", datetime, None),
            "STAT": ("stat", str, "MEDLINE"),
            "PMID": ("pmid", str, "0"),
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
                    attribute, type, default = binds["AB"]
                    try:
                        cont = type(content)
                        self.__setattr__(attribute, content)
                    except Exception, exc:
                        self.__setattr__(attribute, default)
                        print exc
                        pass
                    buff = ""

                # add the attribute
                content = "%s" % raw
                try:
                    attribute, type, default = binds[prefix]
                    try:
                        cont = type(content)
                        self.__setattr__(attribute, content)
                    except Exception, exc:
                        self.__setattr__(attribute, default)
                        print exc
                        pass
                except Exception, exc:
                    print exc
                    pass



class Importer (Importer):
    def __init__(self, path, **options):
    
        self.locale = self.get_property(options, 'locale', 'en_US.UTF-8')

        self.lang,self.encoding = self.locale.split('.')
        file = codecs.open(path, "rU", self.encoding)
        self.documents = Importer._load_documents(file, self.locale)
        self.corpus = corpus.Corpus( name=path, documents=self.documents )

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

            docs += [ document.Document(
                rawContent=model,
                title=model.title,
                targets=[ Target(
                     rawTarget=concat,
                     type=model.abstract,
                     locale=locale
                     )]
                )]
            lines = []
        return docs
        

