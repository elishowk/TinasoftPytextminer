#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

from tina.storage import Storage
# third party module
import yaml
import pprint

# pytextminer package
import PyTextMiner

class StorageTest:

 
    def test_tokenizers(self):
        corpora = PyTextMiner.Corpora()
        for c in self.data['corpus']:
            corpus = PyTextMiner.Corpus( name=c['name'] )
            for doc in c['documents']:
                content = doc['content']
                corpus.documents += [PyTextMiner.Document(
                    rawContent=doc, 
                    title=doc['title'],
                    targets=[PyTextMiner.Target(rawTarget=content,MyLocale='en_US.UTF-8')],
                    )]
            corpora.corpora += [corpus]
        for corpus in corpora.corpora:
            for document in corpus.documents:
                for target in document.targets:
                    print "----- RegexpTokenizer ----\n"
                    target.sanitizedTarget = PyTextMiner.Tokenizer.RegexpTokenizer.sanitize( input=target.rawTarget, forbiddenChars=target.forbiddenChars, emptyString=target.emptyString );
                    
                    #print target.sanitizedTarget
                    target.tokens = PyTextMiner.Tokenizer.RegexpTokenizer.tokenize( text=target.sanitizedTarget, separator=target.separator )
                    target.ngrams = PyTextMiner.Tokenizer.RegexpTokenizer.ngrams(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString,
                    )
                    print "----- NltkTokenizer ----\n"
                    #target.sanitizedTarget = PyTextMiner.Tokenizer.WordPunctTokenizer.sanitize( input=target.rawTarget, forbiddenChars=target.forbiddenChars, emptyString=target.emptyString  );
                    #print target.sanitizedTarget
                    target.tokens = PyTextMiner.Tokenizer.WordPunctTokenizer.tokenize( text=target.sanitizedTarget )
                    target.ngrams = PyTextMiner.Tokenizer.WordPunctTokenizer.ngrams(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString,
                    )
                    #print(target.ngrams)
        return corpora



    def test1_storage(self):

        try:
            f = open("src/t/testdata.yml", 'rU')
        except:
            f = open("t/testdata.yml", 'rU')
        # yaml automatically decodes from utf8
        self.data = yaml.load(f)
        f.close()



        storage = Storage("file:///tmp/pytext_tests", serializer="yaml")
        corpora = self.test_tokenizers()
        storage["corpora"] = corpora
        storage.save()
        del storage

        storage2 = Storage("file:///tmp/pytext_tests", serializer="json")
        corpora = storage2["corpora"]
        for corpus in corpora.corpora:
            print corpus
            for document in corpus.documents:
                print document
                for target in document.targets:
                    print target
                    for ngram in target.ngrams:
                        print ngram







test = StorageTest()
test.test1_storage()

