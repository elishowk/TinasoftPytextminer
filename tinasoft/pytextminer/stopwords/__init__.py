# -*- coding: utf-8 -*-

import codecs
import pickle

class StopWords(object):
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
        self.words = [[]]

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

    def add(self, ngram):
        if not isinstance(ngram,list):
            raise Exception("%s is not a valid ngram (not a list)"%ngram)
        while len(self.words) < len(ngram) + 1:
            self.words+=[[]]
        self.words[len(ngram)] += [ ngram ]

    def __len__(self):
        """ return the length of the ngram"""
        return len(self.words)

    def __getitem__(self, length):
        while len(self.words) < length + 1:
            self.words+=[[]]
        return self.words[length]

    def __contains__(self, word):
        for w in self.words:
            if w == word:
                return True
        return False

    def contains(self, ngram):
        """Check if a given ngram match the base"""
        try:
            t = ngram[0]
        except:
            raise Exception("%s is not a valid ngram"%ngram)
        for stopngram in self[len(ngram)]:
            # stops if the ngram equals stopngram
            # do not stops if types are unicode and str
            # prefered is unicode against unicode
            if len(list(set(ngram).difference(set(stopngram)))) == 0:
                return True
        return False

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