# -*- coding: utf-8 -*-
# used for FS handling
import os
# used for sorting
from operator import itemgetter

from tinasoft import TinaApp, PyTextMiner
from tinasoft.data import basecsv
from tinasoft.pytextminer import tokenizer, tagger, ngram, whitelist
from tinasoft.pytextminer import filtering


# get tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')

class WhitelistFile():
    columns = [
        ("status", "status"),
        ("label", "label"),
        ("postag", "pos tag"),
        ("occs", "total occs"),
        ("length", "length"),
        ("occsn", "total occs ^ length"),
        ("doclist", "doc list"),
        ("corp list", "corp list"),
        ("dbid", "db ID")
    ]
    accept = "w"
    refuse = "s"



class Importer(basecsv.Importer):
    """A class for csv imports of selected ngrams whitelists"""

    # options will be automatically loaded as attributes
    options = {
        'filemodel': WhitelistFile(),
    }

    def __init__(self,
            wlinstance,
            filepath,
            delimiter=',',
            quotechar='"',
            **kwargs
        ):
        self.whitelist = wlinstance
        basecsv.Importer.__init__(self,
            filepath,
            delimiter=',',
            quotechar='"',
            **kwargs
        )

    def _add_whitelist(self, row, dbid, occs):
        """adds a whitelisted ngram"""
        self.whitelist.addEdge( 'NGram', dbid, occs )

    def _add_stopword(self, row, dbid):
        """adds a user defined stop-ngram"""
        pass

    def parse_file(self):
        """Reads a whitelist file and return an object"""
        for row in self.csv:
            try:
                status = row[self.filemodel.columns[0][1]]
                occs = row[self.filemodel.columns[3][1]]
                label = row[self.filemodel.columns[1][1]]
            except KeyError, keyexc:
                _logger.error( keyexc )
                continue
            # calculates db ID from the label, does not trust the file's db id
            dbid = PyTextMiner.getId(label)
            if status == self.filemodel.accept:
                self._add_whitelist(dbid, occs)
            elif status == self.filemodel.refuse:
                self._add_stopword(dbid)
        return self.whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams Whitelists"""

    # options will be automatically loaded as attributes
    options = {
        'filemodel': WhitelistFile(),
    }

    def __init__(self,
            wlinstance,
            filepath,
            delimiter=',',
            quotechar='"',
            **kwargs
        ):
        self.whitelist = wlinstance
        basecsv.Importer.__init__(self,
            filepath,
            delimiter=',',
            quotechar='"',
            **kwargs
        )

    def export_synthesis(self, minOccs=2, max=65000):
        """Dump a lightweight and sorted ngram dict to a file"""
        pass
        #_logger.debug( "saving synthesis whitelist to %s" % self.filepath )
        #TODO
        #self.writeRow( ["label", "total", "max"] )
        #maxsorted = reversed(sorted(ngrams.items(), key=itemgetter(1)))
        #maxsorted = ngrams[:max]
        #del ngrams
        #lines = 0
        #for ng in maxsorted:
        #    self.writeRow([ng[0], ng[1]['tot'], ng[2]['max']])
        #    lines += 1

    #def export_ngrams(self, index, period ):
        """Dump to a whitelist-like file
        the contents of a corpora.Counter instance"""
    #    _logger.debug( "saving partial whitelist to %s for period %s" % \
    #              (self.filepath, period) )
        #self.writeRow( ["status","label","corpus-ngrams w","pos tag","db ID"] )
        #occssorted =reversed(sorted(index[period].items(), key=itemgetter(1)))
    #    for ngid in index[period].iterkeys():
    #        row = [index[period][ngid]['status'],
    #        index[period][ngid]['label'], \
    #        index[period][ngid]['occs'], \
    #        index[period][ngid]['postag'], ngid]
    #        self.writeRow( map(str, row) )

    def export_whitelist(self, storage, periods, corporaid, filters=None, whitelist=None, minOccs=1, ngramlimit=65000):
        """creates and exports a whitelist within selected periods=corpus"""
        self.writeRow([x[1] for x in self.filemodel.columns])

        # basic counters
        ngramcount = 0
        ngramtotal = 0

        corpuscache = []
        ngramcache = {}

        for corpusid in periods:
            # gets a corpus from the storage
            corpusobj = storage.loadCorpus(corpusid)
            if self._corpus_integrity( corpusid, corpusobj ) is False: continue
            ngramtotal += len( corpusobj['edges']['NGram'].keys() )
            corpuscache += [corpusobj]

        _logger.debug( "Exporting %d ngrams to %s" % (ngramtotal, self.filepath) )

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
                        str(float( (ngramcount * 100) / ngramtotal ))
                    )
                # loads an checks ngram
                ng = storage.loadNGram(ngid)
                #if self._ngram_integrity( ng, ngid ) is False: continue
                occs = int(occ)
                n = len( ng['content'] )
                occsn = occs ** n
                if ngid not in ngramcache:
                    # prepares the row
                    tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
                        #columns = [
                        #    ("status", "status"),
                        #    ("label", "label"),
                        #    ("postag", "pos tag"),
                        #    ("occs", "total occs"),
                        #    ("length", "length"),
                        #    ("occsn", "total occs ^ length"),
                        #    ("doclist", "doc list"),
                        #    ("corp list", "corp list"),
                        #    ("dbid", "db ID")
                        #]
                    # prepares the row
                    row = [ "", ng['label'], tag, str(occs), str(n), str(occsn), \
                        " ".join(ng['edges']['Document'].keys()), \
                        " ".join(ng['edges']['Corpus'].keys()), \
                        ng['id'] ]
                    # filtering activated
                    if filters is not None and filtering.apply_filters(ng, filters) is False:
                        # status='s'
                        row[0] = self.refuse
                    if whitelist is not None and ng['id'] in whitelist['edges']:
                        # status='w'
                        row[0] = self.accept
                    ngramcache[ngid] = row
                else:
                    # increments occs and occs^lenght
                    sum = int(ngramcache[ngid][4]) + int(occs)
                    ngramcache[ngid][4] = str(sum)
                    ngramcache[ngid][5] = str( sum ** n )
        del corpuscache
        totalexport = 0
        _logger.debug("Writing export to %s" % self.filepath)
        for ngid in ngramcache.keys():
            if int(ngramcache[ngid][4]) >= minOccs:
                # writes the row to the file
                self.writeRow(ngramcache[ngid])
                del ngramcache[ngid]
                totalexport += 1
        #self.logIntegrity('Corpus')
        #self.logIntegrity('Document')
        #self.logIntegrity('NGram')
        _logger.debug( "Total ngrams exported = %d" % totalexport )
        return self.filepath


    def export_documents( self, storage, periods, corporaid ):
        """exports corpus' documents"""
        for corpusid in periods:
            corpusobj = storage.loadCorpus(corpusid)
            if corpusobj is None:
                _logger.error("unknown corpus %s" % str(corpusid))
                continue
            for docid, occs in corpusobj['edges']['Document'].iteritems():
                doc = storage.loadDocument(docid)
                if doc is None:
                    #_logger.error("Corpus['edges']['NGram'] inconsistency,"\
                    #    +" ngram not found = %s"%ngid)
                    #_logger.error(doc)
                    continue
                if corpusid not in doc['edges']['Corpus'] \
                    or corpusobj['edges']['Document'][docid] != doc['edges']['Corpus'][corpusid]:
                    _logger.error( docid )
                    continue

                ngramedges = len( doc['edges']['NGram'].keys() )
                totalngoccs = sum( doc['edges']['NGram'].values() )
                corpedges = len( doc['edges']['Corpus'].keys() )
                totalcorpoccs = sum( doc['edges']['Corpus'].values() )

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
            self.integrity['NGram'][ngid] += ["NGram %s not in DB" % ngid]
            return False
        if 'Document' not in ng['edges']:
            #_logger.error( "NGram %s inconsistent, no Document edges"%ngid )
            #_logger.error( ng )
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s inconsistent, no Document edges" % ngid]
            return False
        if 'Corpus' not in ng['edges']:
            #_logger.error( "NGram %s inconsistent, no Corpus edges"%ngid )
            #_logger.error( ng )
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += ["NGram %s inconsistent, no Corpus edges" % ngid]
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
            _logger.error("unknown corpus %s" % str(corpusid))
            if corpusid not in self.integrity['Corpus']:
                self.integrity['Corpus'][corpusid] = []
            self.integrity['Corpus'][corpusid] += ["unknown corpus %s" % str(corpusid)]
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
            self.integrity['NGram'][ngid] += ["NGram %s not found in the document %" % (ngid, docid)]
        if ng['edges']['Document'][docid] != storedDoc['edges']['NGram'][ng['id']]:
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += [ "document-ngram weight inconsistency (doc=%s, ng=%s)" % (docid, ngid) ]

        if ng['edges']['Corpus'][corpusid] != occs:
            if ngid not in self.integrity['NGram']:
                self.integrity['NGram'][ngid] = []
            self.integrity['NGram'][ngid] += [ "corpus-ngram weight (in ng %s != in corp %s) inconsistency (corp=%s, ng=%s)" % (str(ng['edges']['Corpus'][corpusid]), str(occs), corpusid, ngid) ]
        return True
        #except Exception, exc:
        #    return True

    def _log_integrity( self, type ):
        for id, obj in self.integrity[type].iteritems():
            msg = "\n".join(obj)
    _logger.error( "%s %s has errors\n%s" % (type, id, msg) )
