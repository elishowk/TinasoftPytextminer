# -*- coding: utf-8 -*-

from tinasoft.data import basecsv
from tinasoft.pytextminer import tokenizer, tagger, cooccurrences
import logging
_logger = logging.getLogger('TinaAppLogger')


class CsvKeyError(KeyError): pass

class Importer (basecsv.Importer):
    """A class for csv imports of selected ngram lists"""
    options = {
        'statusCol': 'status',
        'dbidCol': 'db ID',
        'occsCol': 'corpus-ngram w',
        'labelCol': 'label',
        'accept': 'w',
        'refuse': 's',
        'whitelist': {},
        'stopwords': {},
        'encoding'  : 'utf-8',
    }

    def _addToWhiteList(self, row, dbid, occs):
        if row[self.dbidCol] in self.whitelist:
            self.whitelist[dbid] += occs
        else:
            self.whitelist[dbid] = occs

    def _addStopWord(self, row, dbid):
        pass

    #def _checkRow(self, row, key, lineNum):
        #if key in row:
        #    return True
        #else:
        #    _logger.error("unable to find key " + key + \
        #        " at line " + str(lineNum) + \
        #        " of file " + self.filepath)
        #    return False
        #except KeyError, csve:
        #    _logger.error("unable to find key " + key + \
        #        " at line " + str(lineNum) + \
        #        " of file " + self.filepath)
        #    return False


    def importNGrams(self):
        line=1
        keys=[self.statusCol, self.dbidCol, self.occsCol]
        for row in self.csv:
            try:
                status = row[self.statusCol]
                occs = row[self.occsCol]
                label = row[self.labelCol]
            except KeyError, keyexc:
                _logger.error("Required column missing "
                    + " at line " + str(line) )
                _logger.error( keyexc )
                continue
            dbid = str(abs(hash( label )))
            if status == self.accept:
                self._addToWhiteList(row, dbid, occs)
            elif status == self.refuse:
                self._addStopWord(row, dbid)
            line+=1
        return self.whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of database contents"""
    options = {
        'statusCol': 'status',
        'dbidCol': 'db ID',
        'occsCol': 'occurences',
        'labelCol': 'label',
        'accept': 'w',
        'refuse': 's',
        'encoding': 'utf-8',
        'integrity': { 'NGram' : {}, 'Document': {}, 'Corpus': {} },
    }

    def exportNGrams(self): pass

    def exportCooc(self, storage, periods, whitelist, **kwargs):
        """exports a reconstitued cooc matrix, applying whitelist filtering"""
        for corpusid in periods:
            try:
                generator = storage.selectCorpusCooc( corpusid )
                while 1:
                    ng1,row = generator.next()
                    if whitelist is not None and ng1 not in whitelist:
                        continue
                    for ng2, cooc in row.iteritems():
                        if cooc > row[ng1]:
                            _logger.error( "inconsistency cooc %s %d > %d occur %s"%(ng2,cooc,row[ng1],ng1) )
                        if whitelist is not None and ng2 not in whitelist:
                            continue
                        self.writeRow([ ng1, ng2, str(cooc), corpusid ])
            except StopIteration, si:
                continue
        return self.filepath

    def exportDocuments( self, storage, periods, corporaid ):
        """exports corpus' documents"""
        for corpusid in periods:
            corpusobj = storage.loadCorpus(corpusid)
            if corpusobj is None:
                _logger.error("unknown corpus %s"%str(corpusid))
                continue
            for docid, occs in corpusobj['edges']['Document'].iteritems():
                doc = storage.loadDocument(docid)
                if doc is None:
                    #_logger.error("Corpus['edges']['NGram'] inconsistency,"\
                    #    +" ngram not found = %s"%ngid)
                    #_logger.error(doc)
                    continue
                if corpusid not in doc['edges']['Corpus'] \
                    or corpusobj['edges']['Document'][docid] != doc['edges']['Corpus'][corpusid] :
                    _logger.error( docid )
                    continue

                ngramedges = len( doc['edges']['NGram'].keys() )
                totalngoccs= sum( doc['edges']['NGram'].values() )
                corpedges = len( doc['edges']['Corpus'].keys() )
                totalcorpoccs= sum( doc['edges']['Corpus'].values() )

                row = [ doc['id'], doc['label'], str(ngramedges), \
                        str(totalngoccs), str(corpedges), str(totalcorpoccs), corpusid, corporaid ]
                #_logger.debug( row )
                self.writeRow( row )

    def ngramIntegrity( self, ng, ngid ):
        """checks and logs ngrams object"""
        if ng is None:
            #_logger.error( "NGram %s not in DB"%ngid )
            #_logger.error( ng )
            if ngid not in self.integrity['NGram']:
                 self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s not in DB"%ngid]
            return False
        if 'Document' not in ng['edges']:
            #_logger.error( "NGram %s inconsistent, no Document edges"%ngid )
            #_logger.error( ng )
            if ngid not in self.integrity['NGram']:
                 self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s inconsistent, no Document edges"%ngid]
            return False
        if 'Corpus' not in ng['edges']:
            #_logger.error( "NGram %s inconsistent, no Corpus edges"%ngid )
            #_logger.error( ng )
            if ngid not in self.integrity['NGram']:
                 self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s inconsistent, no Corpus edges"%ngid]
            return False
        return True

    def docIntegrity( self, docid, storedDoc ):
        """checks and logs document object"""
        if storedDoc is None:
            #_logger.error("document object not found " + docid)
            if docid not in self.integrity['Document']:
                self.integrity['Document'][docid] = []
            self.integrity['Document'][docid] += ["document object not found " + docid]
            return False
        return True

    def corpusIntegrity( self, corpusobj, corpusid ):
        """checks and logs corpus object"""
        if corpusobj is None:
            #_logger.error("unknown corpus %s"%str(corpusid))
            if corpusid not in self.integrity['Corpus']:
                self.integrity['Corpus'][corpusid] = []
            self.integrity['Corpus'][corpusid] +=  ["unknown corpus %s"%str(corpusid)]
            return False
        return True

    def ngramEdgesIntegrity( self, ngid, ng, docid, storedDoc, corpusid, occs ):
        """checks and logs ngram edges"""
        if self.docIntegrity( docid, storedDoc ) is False: return False
        if ngid not in storedDoc['edges']['NGram']:
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s not found in the document %"%(ngid,docid)]
        if ng['edges']['Document'][docid] != storedDoc['edges']['NGram'][ng['id']]:
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += [ "document-ngram weight inconsistency (doc=%s, ng=%s)"%(docid,ngid) ]

        if ng['edges']['Corpus'][corpusid] != occs:
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += [ "corpus-ngram weight (in ng %s != in corp %s) inconsistency (corp=%s, ng=%s)"%(str(ng['edges']['Corpus'][corpusid]),str(occs),corpusid,ngid) ]
        return True

    def logIntegrity( self, type ):
        for id, obj in self.integrity[type].iteritems():
            msg = "\n".join(obj)
            _logger.error( "%s %s has errors\n%s"%(type,id,msg) )

    def exportCorpora(self, storage, periods, corporaid, filters=None, whitelist=None):
        """exports selected periods=corpus in a corpora, synthetize importFile()"""
        #rows={}
        _logger.debug("starting to export to %s"%self.filepath)

        self.writeRow(["status","label","pos tag","length","corpus-ngram w","^length",\
            "ng-doc edges","ng-doc w sum","doc list","ng-corpus edges",\
            "ng-corp w sum","corp list","db ID","corpus ID","corpora ID"])
        totalcount=0
        for corpusid in periods:
            # gets a corpus from the generator
            corpusobj = storage.loadCorpus(corpusid)
            if self.corpusIntegrity( corpusid, corpusobj ) is False: continue
            # goes over every ngram in the corpus
            for ngid, occs in corpusobj['edges']['NGram'].iteritems():
                ng = storage.loadNGram(ngid)
                if self.ngramIntegrity( ng, ngid ) is False : continue
                totalcount += 1

                # prepares the row
                tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
                n = len( ng['content'] )
                docedges = len( ng['edges']['Document'].keys() )
                totaldococcs = sum( ng['edges']['Document'].values() )
                corpedges = len( ng['edges']['Corpus'].keys() )
                totalcorpoccs= sum( ng['edges']['Corpus'].values() )
                occs=int(occs)
                occsn=occs**n

                # check document data integrity
                for docid in ng['edges']['Document'].keys():
                    storedDoc = storage.loadDocument(docid)
                    if self.docIntegrity( docid, storedDoc ) is False: continue

                # check corpus data integrity
                if self.ngramEdgesIntegrity( ngid, ng, docid, storedDoc, corpusid, occs ) is False : continue

                row= [ "", ng['label'], tag, str(n), str(occs), str(occsn), \
                    str(docedges), str(totaldococcs), " ".join(ng['edges']['Document'].keys()), \
                    str(corpedges), str(totalcorpoccs), " ".join(ng['edges']['Corpus'].keys()),\
                    ng['id'], str(corpusobj['id']), str(corporaid) ]

                # filtering activated
                if filters is not None and tokenizer.TreeBankWordTokenizer.filterNGrams(ng, filters) is True:
                    # status='s'
                    row[0] = self.refuse
                if whitelist is not None and ng['id'] in whitelist:
                    row[0] = self.accept
                # writes the row to the file
                self.writeRow(row)
            _logger.debug( "corpus %s ngrams edges count = %d"%(corpusid,len(corpusobj['edges']['NGram'].keys())) )
        self.logIntegrity('Corpus')
        self.logIntegrity('Document')
        self.logIntegrity('NGram')
        _logger.debug( "Total ngrams exported = %d"%(totalcount) )
        return self.filepath
