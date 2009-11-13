# -*- coding: utf-8 -*-

#parametres = langue, longueur de ngrams, nombre caractères de chaque mot
#objet chargeant un fichier stopwords donné à l'initialisation ?

class Word (object):
    """A stop word"""
    
    def __init__(self, word, score=1.0):
        """construct a new Stopword from a string. 
        Optional argument: score"""
        self.word = word
        self.score = score

    def __str__(self):
        return self.word.__str__()
        
    def __repr__(self):
        return self.word.__repr__()
        
    def __len__(self):
        """return the number of characters comprised in the word"""
        return len(self.word)

        
class Collection (object):
    """A StopWord Collection"""
    
    def __init__(self, arg, locale='en_US:UTF-8'):
        """Usage:
        
        # you can pass a simple list of words
        c = Collection(["cat","dog"])
        
        # normally you pass a list of n-grams with consistent length 
        # eg. len = 2 for 2-grams
        c = Collection([
                          ["I","like"],
                          ["you","have"]
                       ])
                       
        # but you can also pass a list of n-grams with unconsistent size
        c = Collection([
                          ["dog"],
                          ["I","like"],
                          ["you","have"]
                       ])
            
        # finally, you can pass a full database of ngrams                
        c = Collection([
                         # list of one grams
                         [["cat"],["dog"]],
                         
                         # list of two-grams
                         [["I","like"],["you","have"]]
                       ])
        """
        
        self.locale = locale
        try:
            self.lang, self.encoding = locale.split(':') 
        except:
            self.lang = locale.split(':')[0]
            import sys
            self.encoding = 'utf-8'
	self.encoding = self.encoding.lower()
        self.words = [[]]
        
        # if we have a list (or nested lists) as argument
        if isinstance(arg, list):
            if isinstance(arg[0], list):
                if isinstance(arg[0][0], list):
                    self.words += arg
                else:
                    for ngram in arg:
                        self.add(ngram)
            else:
                self.words += [   [ [w] for w in arg ]   ]
        
        # if we have a file name
        else:
            #try:
            import codecs
            file = codecs.open("%s"%arg, "r", self.encoding)
            for line in file.readlines():
                self.add(line.strip().split(" "))

    def add(self, ngram):
        if not isinstance(ngram,list):
            raise Exception("%s is not a valid ngram (not a list)"%ngram)
        while len(self.words) < len(ngram) + 1:
            self.words+=[[]]

        self.words[len(ngram)] += [ [word.encode(self.encoding) for word in ngram] ]
        
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
            for i in range(0, len(ngram)):
                if stopngram[i] == ngram[i]:
                    return True
        return False
        
    def clean(self, string, length=1):
        """Obsolete - Clean a string from its 1-grams"""
        cleaned = []
        for word in string.split(" "):
            if not self.contains([word]):
                cleaned += [ word ] 
        return " ".join(cleaned)
                
        
class NLTKCollection (Collection):
    """A StopWord Collection"""
    
    def __init__(self, locale='en_US:UTF-8', **options):
        try:
            import nltk
        except:
            raise Exception("you need to install the 'nltk' module to use this collection")
        from nltk.corpus import stopwords   
        lang = locale.split(':')[0]

        words = []
        
        if lang == 'en_US':
            try:
                words += stopwords.words('english')
            except:
                raise Exception("you need to install the 'Stopwords english' corpus for nltk")
        elif lang == 'fr_FR':
            try:
                words += stopwords.words('french')
            except:
                raise Exception("you need to install the 'Stopwords french' corpus for nltk")
        else:
            raise Exception("nltk stopword list only support English language")
            
        Collection.__init__(self, words, locale, **options)

