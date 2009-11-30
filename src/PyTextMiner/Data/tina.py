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
            # decoding & parsing TRY
            try:
                corpusNumber = self.decodeField( doc[self.fields['corpusNumberField']], 'corpusNumberField', None, None )
                del tmpfields['corpusNumberField']
            except Exception as exc:
                print "document parsing exception (no corpus number avalaible) : ", exc
                continue
                #pass

            document = self.parseDocument( doc, tmpfields, corpusNumber )
            if document is None:
                print "skipping document"
                continue
            found = 0
            if self.corpusDict.has_key(corpusNumber) and corpusNumber in corpora.corpora:
                self.corpusDict[ corpusNumber ].documents.append( document.docNum )
                found = 1
            else:
                found = 0
            if found == 1:
                continue
            #print "creating new corpus"
            corpus = PyTextMiner.Corpus(
                name = corpusNumber,
            )
            corpus.documents.append( document.docNum )
            self.corpusDict[ corpusNumber ] = corpus
            corpora.corpora.append( corpusNumber )
        return corpora
            
    def parseDocument( self, doc, tmpfields, corpusNum ):
    
        docArgs = {}
        # parsing TRY
        try: 
            # get required fields
            docNum = self.decodeField( doc[tmpfields[ 'docNumberField' ]], 'docNumberField', None, corpusNum )
            content = self.decodeField( doc[tmpfields[ 'contentField' ]], 'contentField', docNum, corpusNum )
            del tmpfields['docNumberField']
            del tmpfields['contentField']
        except Exception as exc:
            print "Error parsing doc %d from corpus %d : %s\n", docNum, corpusNum, exc
            return None
            
        # parsing optional fields loop and TRY
        for key, field in tmpfields.iteritems():
            try:
                docArgs[ key ] = self.decodeField( doc[ field ], field, docNum, corpusNum )
            except Exception as exc:
                print "warning : unable to parse optional field",  docNum, corpusNum, exc

        document = PyTextMiner.Document(
            rawContent=content,
            docNum=docNum,
            ngramMin=self.minSize,
            ngramMax=self.maxSize,
            **docArgs
        )
        #print document.ngramMin, document.ngramMax, document.docNum, document.rawContent
        self.docDict[ docNum ] = document
        return document

    def decodeField( self, field, fieldName, docNum=None, corpusNumber=None ):
        try:
            return self.decode( field )
        # TODO NOT used because of errors arg in self.decode
        except UnicodeDecodeError as uexc:
            print "Error decoding field in document from corpus\n", fieldName, docNum, corpusNumber, uexc
            return u'\ufffd'
