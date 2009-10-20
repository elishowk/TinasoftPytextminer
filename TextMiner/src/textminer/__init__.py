__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:30:11 PM$"
"""TextMiner Module"""

# time
from time import gmtime, mktime
from chardet import detect as detect_encoding

class TextMiner:
    """TextMiner"""
    def __init__(self):
        pass

class Corpus:
    """a Corpus of Documents"""
    def __init__(self, name, documents=[]):
        self.name = name
        self.documents = documents
        
class Document:
    """a single Document"""
    def __init__(self, corpus, content="", title="", timestamp=mktime(gmtime()), targets=[]):
        """ Document constructor.
        arguments: corpus, content, title, timestamp, targets"""
        self.corpus = corpus
        self.title = title
        self.timestamp = timestamp
        self.targets = targets

class Target:
    """a text Target in a Document"""
    def __init__(self, target, type=None, ngrams=[], minSize=1, maxSize=3):
        """Text Target constructor"""
        self.type = type
        self.target = target
        self.sanitizedTarget = None
        self.ngrams = ngrams
        self.minSize = minSize
        self.maxSize = maxSize

    def _build_sanitizedTarget(self):
        """Build the sanitized target

	@return str: text
	"""
        text = decode_utf8(self.target)
	# cleaning anything except words
	text =~ s/\W/ /g;
	return text
}



class NGram:
    """an ngram"""
    def __init__(self, ngram, occurences=0):
        self.occurences = occurences
        self.ngram = ngram
    def __len__(self):
        """ return the length of the ngram"""
        return 0 # call with  len(  instance_of_NGRam  )
