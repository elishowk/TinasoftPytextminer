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
    
    def __init__(self, arg, lang='en_US', encoding='utf-8'):
        self.lang = lang
        self.encoding = encoding
        
        if arg is str:
            arg = []
            file = open(arg, "r")
            for line in file.readlines():
                arg += [ line  ]

        self.words = arg
            

    def __len__(self):
        """ return the length of the ngram"""
        return len(self.words)

    def __str__(self):
        return self.str.encode(self.encoding)
        
    def __repr__(self):
        return self.str.encode(self.encoding)
 
    def __contains__(self, word):
        for w in self.words:
            if w == word:
                return True
        return False

    def clean(self, string):
        """Clean a string from its stopwords"""
        cleaned = []
        for word in string.split(" "):
            if not word in self.words:
                cleaned += [ word ]
        return " ".join(cleaned)
                
        
class NLTKCollection (Collection):
    """A StopWord Collection"""
    
    def __init__(self, lang='en_US', **options):
        
        # ntlk's stopwords worpus only support english words
        if lang != 'en_US':
            raise Exception("nltk stopword list only support English language")
            
        words = []
        try:
            import nltk
        except:
            raise Exception("you need to install the 'nltk' module to use this collection")
        try:
            from nltk.corpus import stopwords
            words += stopwords.words()
        except:
            raise Exception("you need to install the 'Stopwords' corpus for nltk")

        Collection.__init__(self, words, lang, **options)

