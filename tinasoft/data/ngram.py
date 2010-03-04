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
        'statusCol':'status',
        'dbidCol':'db ID',
        'occsCol':'occurences',
        'labelCol':'label',
        'accept':'w',
        'refuse':'s',
        'encoding'  : 'utf-8',
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
            if corpusobj is None:
                _logger.error("unknown corpus %s"%str(corpusid))
                continue
            # goes over every ngram in the corpus
            for ngid, occs in corpusobj['edges']['NGram'].iteritems():
                totalcount += 1
                ng = storage.loadNGram(ngid)
                if ng is None:
                    #_logger.error("Corpus['edges']['NGram'] inconsistency,"\
                    #    +" ngram not found = %s"%ngid)
                    _logger.error(ng)
                    return
                # prepares the row
                tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
                n=len(ng['content'])
                docedges = len( ng['edges']['Document'].keys() )
                totaldococcs= sum( ng['edges']['Document'].values() )
                corpedges = len( ng['edges']['Corpus'].keys() )
                totalcorpoccs= sum( ng['edges']['Corpus'].values() )
                occs=int(occs)
                occsn=occs**n

                # check document data integrity
                for docid in ng['edges']['Document'].keys():
                    storedDoc = storage.loadDocument(docid)
                    if storedDoc is None:
                        _logger.error("document object not found " + docid)
                        return
                    if ng['id'] not in storedDoc['edges']['NGram']:
                        _logger.error( "NGram %s not found in the document %"%(ngid,docid) )
                    if ng['edges']['Document'][docid] != storedDoc['edges']['NGram'][ng['id']]:
                        _logger.error( "document-ngram weight inconsistency (doc=%s, ng=%s)"%(docid,ngid) )

                # check corpus data integrity
                if ng['edges']['Corpus'][corpusid] != occs:
                    print ngid
                    _logger.error( "corpus-ngram weight (in ng %s != in corp %s) inconsistency (corp=%s, ng=%s)"%(str(ng['edges']['Corpus'][corpusid]),str(occs),corpusid,ngid) )

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
            _logger.debug( "corpusid ngrams edges count = " + str(len(corpusobj['edges']['NGram'].keys())) )
        _logger.debug( "Total ngrams exported = " + str(totalcount) )
        return self.filepath
