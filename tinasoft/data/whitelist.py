# -*- coding: utf-8 -*-
from operator import itemgetter

import os
from operator import itemgetter

from tinasoft import TinaApp
from tinasoft.data import basecsv
from tinasoft.pytextminer import tokenizer, tagger

# logger
import logging
_logger = logging.getLogger('TinaAppLogger')

class CsvKeyError(KeyError): pass

class WhitelistHandler():

    keys = ["status", "label", "corpus-ngrams w", "pos tag", "db ID"]
    whitelist = {}
    ngrams = {}

    def walk(self, directory):
        periods = {}
        files = os.listdir( directory )
        for file in files:
            #if not os.path.isfile(file):
            #    print "not a file"
            #    continue
            namesplit = file.split(".", 2)
            if len(namesplit) < 2:
                continue
            if namesplit[1] != "csv":
                continue
            parsedname = namesplit[0].split("-", 3)
            if len(parsedname) != 3:
                continue
            if parsedname[1][:4] not in periods:
                periods[parsedname[1][:4]] = [os.path.join( directory, file)]
            else:
                periods[parsedname[1][:4]] += [os.path.join( directory, file)]
        for period, files in periods.iteritems():
            #if period in ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010']: continue
            for file in files:
                self.reducer( period, self.mapper(file) )
            #path = self.getPath(period)
            #exporter = Exporter( path )
            #exporter.export_ngrams( self.whitelist, period )
            del self.whitelist[period]
        synth = Exporter( os.path.join( directory, "synthesis-whitelist.csv" ) )
        synth.exportSynthesis( self.ngrams )



    def getPath(self, period, fileprefix=None):
        if fileprefix is None:
            return "%s-whitelist.csv"%period
        if isinstance(fileprefix, int):
            path = "%d-%s-whitelist.csv"%(fileprefix,period)
        elif isinstance(fileprefix,str):
            path = "%s-%s-whitelist.csv"%(fileprefix,period)
        return path


    def mapper(self, path, whitelist=None):
        """parse a whitelist file and returns its dict object"""
        if whitelist is None:
            whitelist = {}
        reader = basecsv.Importer(path)
        for row in reader.csv:
            #_logger.debug(row)
            try:
                status = row[self.keys[0]]
                label = row[self.keys[1]]
                occs = int(row[self.keys[2]])
                tag = row[self.keys[3]]
                # manual recalculate db ID
                dbid = str(abs(hash( label )))
                # aggregation in whitelist
                if dbid in whitelist:
                    whitelist[dbid]['occs'] += occs
                else:
                    whitelist[dbid] = {
                        'label': label,
                        'status': status,
                        'occs': occs,
                        'postag': tag
                    }
                #_logger.debug("end of mapper")
            except KeyError, keyexc:
                _logger.error("Required column missing "
                    + " at line " + str(line) )
                _logger.error( keyexc )
                continue
        return whitelist


    def reducer(self, period, whitelist):
        """aggregate a whitelist into a given period and updates global ngrams synthesis"""
        if period not in self.whitelist:
            self.whitelist[period] = {}
        for dbid, ng in whitelist.iteritems():
            if ng['label'] not in self.ngrams:
                self.ngrams[ng['label']] = {
                    'max':0,
                    'tot':0
                }
            if self.ngrams[ng['label']]['max'] < ng['occs']:
                self.ngrams[ng['label']]['max'] = ng['occs']
            self.ngrams[ng['label']]['tot'] += ng['occs']
            self.ngrams = dict(reversed(sorted(self.ngrams.items(), key=itemgetter(1))))
        _logger.debug("end of reduce")
            #if dbid in self.whitelist[period]:
            #    self.whitelist[period][dbid]['occs'] += ng['occs']
            #else:
            #    self.whitelist[period][dbid] = ng




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

    def _add_whitelist(self, row, dbid, occs):
        """Classify a whitelist ngram"""
        if row[self.dbidCol] in self.whitelist:
            self.whitelist[dbid] += occs
        else:
            self.whitelist[dbid] = occs

    def _add_stopword(self, row, dbid):
        pass

    def import_whitelist(self):
        """Reads a whitelist file and return an object"""
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
            # manual recalculate db ID
            dbid = str(abs(hash( label )))
            if status == self.accept:
                self._add_whitelist(row, dbid, occs)
            elif status == self.refuse:
                self._add_stopword(row, dbid)
            line+=1
        return self.whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams"""
    options = {
        'statusCol': 'status',
        'dbidCol': 'db ID',
        'occsCol': 'corpus-ngram w',
        'labelCol': 'label',
        'accept': 'w',
        'refuse': 's',
        'encoding': 'utf-8',
        'integrity': { 'NGram' : {}, 'Document': {}, 'Corpus': {} },
    }

    columns = ["status","label","pos tag","length","corpus-ngram w","^length",\
            "doc list","corp list","db ID","corpora ID"]

    #columns = ["status","label","pos tag","length","corpus-ngram w","^length",\
    #        "ng-doc edges","ng-doc w sum","doc list","ng-corpus edges",\
    #        "ng-corp w sum","corp list","db ID","corpus ID","corpora ID"]

    def exportSynthesis(self, ngrams, max=65000):
        """Dump to a file an ngram dict after sorting"""
        _logger.debug( "saving synthesis whitelist to %s"%self.filepath )
        self.writeRow( ["label","total","max"] )
        maxsorted = reversed(sorted(ngrams.items(), key=itemgetter(1)))
        maxsorted = ngrams[:max]
        del ngrams
        lines = 0
        for ng in maxsorted:
            self.writeRow([ng[0], ng[1]['tot'], ng[2]['max']])
            lines += 1

    def export_ngrams(self, index, period ):
        """Dump to a whitelist-like file
        the contents of a corpora.Counter instance"""
        _logger.debug( "saving partial whitelist to %s for period %s"%\
            (self.filepath, period) )
        #self.writeRow( ["status","label","corpus-ngrams w","pos tag","db ID"] )
        #occssorted =reversed(sorted(index[period].items(), key=itemgetter(1)))
        for ngid in index[period].iterkeys():
            row=[index[period][ngid]['status'],
            index[period][ngid]['label'], \
            index[period][ngid]['occs'], \
            index[period][ngid]['postag'], ngid]
            self.writeRow( map(str, row) )

    def export_cooc(self, storage, periods, whitelist, **kwargs):
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

    def export_whitelist(self, storage, periods, corporaid, filters=None, whitelist=None, ngramlimit=65000, minOccs=1):
        """exports selected periods=corpus in a corpora, synthetize importFile()"""
        self.writeRow(self.columns)

        # basic counters
        ngramcount=0
        ngramtotal=0

        corpuscache = []
        ngramcache = {}

        for corpusid in periods:
            # gets a corpus from the storage
            corpusobj = storage.loadCorpus(corpusid)
            if self._corpus_integrity( corpusid, corpusobj ) is False: continue
            ngramtotal += len( corpusobj['edges']['NGram'].keys() )
            corpuscache += [corpusobj]

        _logger.debug( "Exporting %d ngrams to %s"%(ngramtotal,self.filepath) )

        for corpusobj in corpuscache:
            # sorts ngrams by occs
            #sortedngrams = reversed(sorted(corpusobj['edges']['NGram'].items(), key=itemgetter(1)))
            # goes over every ngram in the corpus
            for ngid, occ in corpusobj['edges']['NGram'].iteritems():
                # notifies progression and stops if limit exceeded
                ngramcount += 1
                #if ngramlimit <= ngramcount: break
                if ngramcount % 500 == 0:
                    TinaApp.notify( None,
                        'tinasoft_runExportCorpora_running_status',
                        str(float( (ngramcount*100) / ngramtotal ))
                    )
                # loads an checks ngram
                ng = storage.loadNGram(ngid)
                if self._ngram_integrity( ng, ngid ) is False : continue
                occs=int(occ)
                n = len( ng['content'] )
                occsn=occs**n
                if ngid not in ngramcache:
                    # prepares the row
                    tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
                    #docedges = len( ng['edges']['Document'].keys() )
                    #totaldococcs = sum( ng['edges']['Document'].values() )
                    #corpedges = len( ng['edges']['Corpus'].keys() )
                    #totalcorpoccs= sum( ng['edges']['Corpus'].values() )
                    # check document data integrity
                    #for docid in ng['edges']['Document'].keys():
                    #    storedDoc = storage.loadDocument(docid)
                    #    if self.docIntegrity( docid, storedDoc ) is False: continue

                    # check corpus data integrity
                    #if self.ngramEdgesIntegrity( ngid, ng, docid, storedDoc, corpusid, occs ) is False : continue
                    row= [ "", ng['label'], tag, str(n), str(occs), str(occsn), \
                        " ".join(ng['edges']['Document'].keys()), \
                        " ".join(ng['edges']['Corpus'].keys()),\
                        ng['id'], str(corporaid) ]
                    #row= [ "", ng['label'], tag, str(n), str(occs), str(occsn), \
                    #    str(docedges), str(totaldococcs), " ".join(ng['edges']['Document'].keys()), \
                    #    str(corpedges), str(totalcorpoccs), " ".join(ng['edges']['Corpus'].keys()),\
                    #    ng['id'], str(corpusobj['id']), str(corporaid) ]

                    # filtering activated
                    if filters is not None and tokenizer.TreeBankWordTokenizer.filterNGrams(ng, filters) is False:
                        # status='s'
                        row[0] = self.refuse
                    if whitelist is not None and ng['id'] in whitelist:
                        # status='w'
                        row[0] = self.accept
                    ngramcache[ngid]=row
                else:
                    # increments occs and occs^lenght
                    sum = int(ngramcache[ngid][4]) + int(occs)
                    ngramcache[ngid][4] = str(sum)
                    ngramcache[ngid][5] = str( sum**n )
        del corpuscache
        totalexport = 0
        _logger.debug("Writing export to %s"%self.filepath)
        for ngid in ngramcache.keys():
            if int(ngramcache[ngid][4]) >= minOccs:
                # writes the row to the file
                self.writeRow(ngramcache[ngid])
                del ngramcache[ngid]
                totalexport+=1
        #self.logIntegrity('Corpus')
        #self.logIntegrity('Document')
        #self.logIntegrity('NGram')
        _logger.debug( "Total ngrams exported = %d"%totalexport )
        return self.filepath


    def export_documents( self, storage, periods, corporaid ):
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

    def _ngram_integrity( self, ng, ngid ):
        """checks and logs ngrams object"""
        #try:
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
        #except Exception, exc:
        #    return True

    def _doc_integrity( self, docid, storedDoc ):
        """checks and logs document object"""
        #try:
        if storedDoc is None:
            #_logger.error("document object not found " + docid)
            if docid not in self.integrity['Document']:
                self.integrity['Document'][docid] = []
            self.integrity['Document'][docid] += ["document object not found " + docid]
            return False
        return True
        #except Exception, exc:
        #    return True


    def _corpus_integrity( self, corpusid, corpusobj ):
        """checks and logs corpus object"""
        #try:
        if corpusobj is None:
            _logger.error("unknown corpus %s"%str(corpusid))
            if corpusid not in self.integrity['Corpus']:
                self.integrity['Corpus'][corpusid] = []
            self.integrity['Corpus'][corpusid] +=  ["unknown corpus %s"%str(corpusid)]
            return False
        return True
        #except Exception, exc:
        #    return True

    def _ngram_edges_integrity( self, ngid, ng, docid, storedDoc, corpusid, occs ):
        """checks and logs ngram edges"""
        #try:
        if self._doc_integrity( docid, storedDoc ) is False: return False
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
        #except Exception, exc:
        #    return True

    def _log_integrity( self, type ):
        for id, obj in self.integrity[type].iteritems():
            msg = "\n".join(obj)
            _logger.error( "%s %s has errors\n%s"%(type,id,msg) )
