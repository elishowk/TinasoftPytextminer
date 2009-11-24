# -*- coding: utf-8 -*-

import codecs
import csv
from datetime import datetime

import PyTextMiner

class Exporter (PyTextMiner.Data.Exporter):

    def __init__(self,
                filepath,
                #corpus,
                delimiter = u',',
                quotechar = '"',
                locale = 'en_US.UTF-8',
                dialect = 'excel'):
        self.filepath = filepath
        #self.corpus = corpus
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.locale = locale
        self.encoding =  self.locale.split('.')[1].lower()
        self.dialect = dialect


    def objectToCsv( self, objlist, columns ):
        file = codecs.open(self.filepath, "w", self.encoding, 'xmlcharrefreplace' )
        file.write( self.delimiter.join( columns ) + "\n" )
        def mapping( att ) :
            print type(att)
            print att
            s = str(att)
            return self.encode( s )
        for obj in objlist:
            attributes = [getattr(obj, col) for col in columns]
            file.write( self.delimiter.join( map( mapping, attributes ) ) + "\n" )
            #file.write( self.delimiter.join( map( self.encode, map( str, attributes ) ) ) + "\n" )

    def csvFile( self, columns, rows ):
        file = codecs.open(self.filepath, "w", encoding=self.encoding )
        file.write( self.delimiter.join( columns ) + "\n" )
        for row in rows:
            file.write( self.delimiter.join( map( self.encode, map( str, row ) ) ) + "\n" )


class Importer (PyTextMiner.Data.Importer):

    options = {
            'fields' : {
                'titleField' : 'doc_titl',
                'dateEndField' : None,
                'dateStartField' : None,
                'contentField' : 'doc_abst',
                'authorField' : 'doc_acrnm',
                'corpusNumberField' : 'corp_num',
                'docNumberField' : 'doc_num',
                'index1Field' : 'index_1',
                'index2Field' : 'index_2',
                'keywordsField' : None,
            }

    }

    def __init__(self,
            filepath,
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
            **kwargs 
        ):
        self.filepath = filepath
        self.load_options( kwargs )
        # locale management
        self.locale = locale
        self.encoding = locale.split('.')[1].lower()
        # CSV format
        self.delimiter = delimiter
        self.quotechar = quotechar
        # Tokenizer args
        self.minSize = minSize
        self.maxSize = maxSize
        
        f1 = self.open( filepath )
        tmp = csv.reader(
                f1,
                delimiter=self.delimiter,
                quotechar=self.quotechar
        )
        self.fieldNames = tmp.next()
        del f1
        
        f2 = self.open( filepath )
        self.csv = csv.DictReader(
                f2,
                self.fieldNames,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
        self.csv.next()
        del f2
        self.docDict = {}
        self.corpusDict = {}

    def open( self, filepath ):
        return codecs.open(filepath,'rU', errors='replace' )
    
    #def decode( self, text ):
    #    # replacement char = \ufffd
    #    return decode( text, self.encoding, 'xmlcharrefreplace' )

    def corpora( self, corpora ):
        for doc in self.csv:
            tmpfields=dict(self.fields)
            try:
                corpusNumber = self.decode( doc[self.fields['corpusNumberField']] )
                del tmpfields['corpusNumberField']
            except Exception, exc:
                print "document parsing exception : ", exc
                continue
                pass
            document = self.parseDocument( doc, tmpfields )
            if document is None:
                print "skipping document"
                continue
            found = 0
            if self.corpusDict.has_key(corpusNumber) and corpusNumber in corpora.corpora:
                #print "existing coprus, adding document :", document.docNum
                self.corpusDict[ corpusNumber ].documents.add( document.docNum )
                found = 1
            else:
                found = 0
            if found == 1:
                continue
            #print "creating new corpus"
            corpus = PyTextMiner.Corpus(
                name = corpusNumber,
            )
            corpus.documents.add( document.docNum )
            self.corpusDict[ corpusNumber ] = corpus
            corpora.corpora.add( corpusNumber )
        return corpora
            
    def parseDocument( self, doc, tmpfields ):
    
        docArgs = {}
        try: 
            # get required fields
            docNum = self.decode( doc[tmpfields[ 'docNumberField' ]] )
            content = self.decode( doc[tmpfields[ 'contentField' ]] )
            del tmpfields['docNumberField']
            del tmpfields['contentField']
            try:
                # get optional fields
                for key, field in tmpfields.iteritems():
                    docArgs[ key ] = self.decode( doc[ field ] )
            except:
                pass
        except Exception, exc:
            print "document parsing exception : ", exc
            return None
            pass

        document = PyTextMiner.Document(
            rawContent=content,
            #title=title,
            #author=author,
            docNum=docNum,
            #date=date,
            #keywords=keywords,
            #index1=index1,
            #index2=index2,
            ngramMin=self.minSize,
            ngramMax=self.maxSize,
            **docArgs
        )
        #print document.ngramMin, document.ngramMax, document.docNum, document.rawContent
        self.docDict[ docNum ] = document
        return document
