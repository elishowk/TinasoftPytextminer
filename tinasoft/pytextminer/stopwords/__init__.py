# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__="elishowk@nonutc.fr"
import codecs
import pickle
from tinasoft.pytextminer import ngram as NG
import logging
_logger = logging.getLogger('TinaAppLogger')

class StopWords(object):
    """StopWords"""

    def __init__(self, uri, locale='en_US.UTF-8'):
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
            self.lang = 'en_US'
            self.encoding = 'utf-8'
        self.encoding = self.encoding.lower()
        self.words = [{}]

        if isinstance(uri, list):
            self.__list(uri)
        else:
            protocol, path = uri.split("://")
            if protocol == "file":
                self.__file(path)
            #elif protocol == "http":
            #    self.__file("http://"+path)
            elif protocol == "nltk":
                self.__nltk(path)
            elif protocol == "pickle":
                self.__pickle(path)
            self.protocol = protocol

    def __pickle(self, path):
        """
        Loads a pickled StopWords object
        """
        file = codecs.open( path , "r" )
        self.words = pickle.load( file )

    def __nltk(self, lang):
        """
        Loads NLTK's stopworks
        """
        try:
            import nltk
            from nltk.corpus import stopwords
            for word in stopwords.words(lang):
                self.add([word])
        except ImportError, err:
            raise Exception("you need to install the 'stopwords' corpus for nltk")

    def __file(self, uri):
        """
        Loads stopwords from a text file
        """
        for line in codecs.open("%s"%uri, "rU", self.encoding, errors='replace').readlines():
            self.add( line.strip().split(" ") )

    def __list(self, lst):
        """
        Loads stopworsd from a list variable
        """
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
        """
        Adds a stop-ngram to the object
        @stopng must be a list of words
        """
        if not isinstance(stopng, list):
            raise Exception("%s is not a valid ngram (not a list)"%stopng)
        while len(self.words) < len(stopng) + 1:
            self.words+=[{}]
        stopngobj = NG.NGram( stopng )
        self.words[len(stopng)][ stopngobj['label'] ] = 1
        return stopngobj

    def __len__(self):
        """
        return the max length of ngrams present in the object
        """
        return len(self.words)

    def __getitem__(self, length):
        """
        Access stopwords in a dict style
        """
        while len(self.words) < length + 1:
            self.words+=[{}]
        return self.words[length]

    def contains(self, ngramobj):
        """
        Checks a ngram object against the stop base using NGram['id']
        """
        for label in ngramobj['edges']['label'].keys():
            if label in self[len(label.split(" "))]:
                return True
        return False

    def test(self, ngramobj):
        """
        inverse of self.contains
        """
        return not self.contains( ngramobj )


    def savePickle( self, filepath ):
        """
        copy the stopwords in-memory database to file
        """
        file = codecs.open( filepath, "w+" )
        pickle.dump( self.words, file )

class StopWordFilter(StopWords):
    """
    variaton of StopWords to fit the Filter model
    used to filter ngrams given a list of words
    eg : dynamically user defined stopword
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
