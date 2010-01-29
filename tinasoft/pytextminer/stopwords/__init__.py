# -*- coding: utf-8 -*-

import codecs
import pickle
from tinasoft.pytextminer import ngram as NG
from tinasoft.data import Reader

class StopWords( object ):
    """StopWords"""

    def __init__(self, arg, locale='en_US.UTF-8'):
        """
        # you can pass a simple list of words
        c = StopWords(["cat","dog"])

        # normally you pass a list of n-grams with consistent length
        # eg. len = 2 for 2-grams
        c = StopWords([  ["I","like"],
                         ["you","have"] ])

        # but you can also pass a list of n-grams with unconsistent size
        c = StopWords([  ["dog"],
                         ["I","like"],
                         ["you","have"] ])

        # finally, you can pass a full database of ngrams
        c = StopWords([    [["cat"],["dog"]],

                           [["I","like"],["you","have"]]])"""

        self.locale = locale
        try:
            self.lang, self.encoding = locale.split('.')
        except:
            self.lang = locale.split('.')[0]
            self.encoding = 'utf-8'
        self.encoding = self.encoding.lower()
        self.words = [{}]

        if isinstance(arg, list):
            self.__list(arg)
        else:
            protocol, path = arg.split("://")
            if protocol == "file":
                self.__file(path)
            elif protocol == "http":
                self.__file("http://"+path)
            elif protocol == "nltk":
                self.__nltk(path)
            elif protocol == "pickle":
                self.__pickle(path)

            self.protocol = protocol

    def __pickle(self, path):
        file = codecs.open( path , "r" )
        self.words = pickle.load( file )

    def __nltk(self, lang):
        try:
            import nltk.corpus
        except:
            raise Exception("you need to install NLTK library")
        try:
            import nltk
            nltk.data.path = ['shared/nltk_data']
            from nltk.corpus import stopwords
            for word in stopwords.words(lang):
                self.add([word])
        except ImportError, err:
            raise Exception("you need to install the 'stopwords' corpus for nltk")

    def __file(self, arg):
        for line in codecs.open("%s"%arg, "r", self.encoding).readlines():
            self.add(line.strip().split(" "))

    def __list(self, lst):
        if isinstance(lst[0], list):
            if isinstance(lst[0][0], list):
                for length in lst:
                    for ngram in length:
                        self.add(ngram)
            else:
                for ngram in lst:
                    self.add(ngram)
        else:
            for word in lst:
                self.add([word])

    def add(self, stopng):
        """ngram must be a list of words"""
        if not isinstance(stopng,list):
            raise Exception("%s is not a valid ngram (not a list)"%stopng)
        while len(self.words) < len(stopng) + 1:
            self.words+=[{}]
        stopngobj = NG.NGram( stopng )
        #print stopngobj.id, type(stopngobj.id), stopngobj.label
        self.words[len(stopng)][ stopngobj['id'] ] = stopngobj
        return stopngobj

    def __len__(self):
        """ return the length of the ngram"""
        return len(self.words)

    def __getitem__(self, length):
        while len(self.words) < length + 1:
            self.words+=[{}]
        return self.words[length]

    def contains(self, ngramobj):
        """Check a ngram object against the stop base using NGram['id']"""
        #print "testing id = ", ngramobj['id'], ngramobj['content']
        if ngramobj['id'] in self[len(ngramobj['content'])]:
            #print "stopword !", type(ngramobj['id']), ngramobj['id']
            return True
        else:
            return False

    ### obsolete
    def cleanText( self, string ):
        """Obsolete - Clean a string from its 1-grams"""
        cleaned = []
        for word in string.split(" "):
            if not self.contains([word]):
                cleaned += [ word ]
        return " ".join(cleaned)

    def savePickle( self, filepath ):
        """copy the stopwords in-memory database to file"""
        file = codecs.open( filepath, "w" )
        pickle.dump( self.words, file )

class StopWordFilter(StopWords):
    """
    used for filtering ngrams
    given a file or a list
    """
    def __init__(self, stopngrams):
        StopWords.__init__(self, stopngrams)

    def any(self, nggenerator):
        try:
            record = nggenerator.next()
            while record:
                if self.contains( record[1] ) is False:
                    yield record
                record = nggenerator.next()
        except StopIteration, si:
            return

