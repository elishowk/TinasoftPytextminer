# -*- coding: utf-8 -*-

from tinasoft.data import basecsv
from tinasoft.pytextminer import tokenizer
import logging
_logger = logging.getLogger('TinaAppLogger')


class CsvKeyError(KeyError): pass

class Importer (basecsv.Importer):
    """A class for csv imports of selected ngram lists"""
    options = {
        'statusCol':'status',
        'dbidCol':'db ID',
        'occsCol':'occs',
        'labelCol':'label',
        'accept':'w',
        'refuse':'s',
        'whitelist':{},
        'stopwords':{},
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
                _logger.error("unable to find a column "
                    " at line " + str(line) + \
                    " of file " + self.filepath)
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

    def exportCooc(self, db, periods, whitelist, **kwargs):
        for corpusid in periods:
            nodes = {}
            try:
                generator = db.selectCorpusCooc(corpusid)
                while 1:
                    key,row = generator.next()
                    id,month = key
                    if id not in whitelist:
                        continue
                    if id not in nodes:
                        nodes[id] = {
                            'edges' : {}
                        }
                    for ngram, cooc in row.iteritems():
                        if ngram not in whitelist:
                            continue
                        if ngram in nodes[id]['edges']:
                            nodes[id]['edges'][ngram] += cooc
                        else:
                            nodes[id]['edges'][ngram] = cooc
            except StopIteration, si:
                for ng1 in nodes.iterkeys():
                    for ng2, cooc in nodes[ng1]['edges'].iteritems():
                        self.writeRow([ ng1, ng2, str(cooc), corpusid ])
        return self.filepath

    def exportCorpora(self, storage, periods, corporaid, filters=None, whitelist=None):
        """exports selected periods=corpus in a corpora, synthetize importFile()"""
        rows={}
        _logger.debug("starting to write to %s"%self.filepath)

        self.writeRow(["status","label","length","corpus-ngram w","^length",\
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
                    continue
                # prepares the row
                n=len(ng['content'])
                docedges = len( ng['edges']['Document'].keys() )
                totaldococcs= sum( ng['edges']['Document'].values() )
                corpedges = len( ng['edges']['Corpus'].keys() )
                totalcorpoccs= sum( ng['edges']['Corpus'].values() )
                occs=int(occs)
                occsn=occs**n
                if len(ng['edges']['Document'].keys()) > 1:
                    _logger.debug(row)
                row= [ "", ng['label'], str(n), str(occs), str(occsn), \
                    str(docedges), str(totaldococcs), " ".join(ng['edges']['Document'].keys()), \
                    str(corpedges), str(totalcorpoccs), " ".join(ng['edges']['Corpus'].keys()),\
                    ng['id'], str(corpusobj['id']), str(corporaid) ]
                # filtering activated
                if filters is not None and tokenizer.TreeBankWordTokenizer.filterNGrams(ng, filters) is True:
                    # status='s'
                    row[0] = self.refuse
                if whitelist is not None and ng['id'] not in whitelist:
                    row[0] = self.accept
                # writes the row to the file
                self.writeRow(row)
            _logger.debug( "corpusid ngrams edges count = " + str(len(corpusobj['edges']['NGram'].keys())) )
        _logger.debug( "Total ngrams exported = " + str(totalcount) )
        return self.filepath
