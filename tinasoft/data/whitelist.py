# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

# used for FS handling
import os

from tinasoft import TinaApp
from tinasoft.pytextminer import PyTextMiner
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
        ("corplist", "corp list"),
        ("dbid", "db ID")
    ]
    accept = "w"
    refuse = "s"



class Importer(basecsv.Importer):
    """A class for csv imports of selected ngrams whitelists"""

    filemodel = WhitelistFile()

    def _add_whitelist(self, dbid, occs):
        """adds a whitelisted ngram"""
        self.whitelist.addEdge( 'NGram', dbid, occs )

    def _add_stopword(self, dbid, occs):
        """adds a user defined stop-ngram"""
        self.whitelist.addEdge( 'StopNGram', dbid, occs )

    def parse_file(self):
        """Reads a whitelist file and returns the updated object"""
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
                self._add_stopword(dbid, occs)
        return self.whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams Whitelists"""

    filemodel = WhitelistFile()

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

    def export_whitelist(self, storage, periods, new_whitelist_label, filters=None, compl_whitelist=None, ngramlimit=65000, minOccs=1 ):
        """creates and exports a whitelist within selected periods=corpus"""
        self.writeRow([x[1] for x in self.filemodel.columns])
        _logger.debug( "Creating the new whitelist" )
        newwl = whitelist.Whitelist( new_whitelist_label, None, new_whitelist_label )
        ngrams = newwl.create( storage, periods, filters, compl_whitelist )
        # basic monitoring counters
        ngramcount = totalexported = 0
        ngramtotal = len(ngrams.keys())
        _logger.debug( "Exporting %d ngrams to %s" % (ngramtotal, self.filepath) )

        for ng in ngrams.itervalues():
            ngramcount += 1
            occs = newwl['edges']['NGram'][ng['id']]
            _logger.debug( "total = %d"%occs )
            if not occs >= minOccs:
                continue
            totalexported += 1
            occsn = newwl['edges']['Normalized'][ng['id']]
            _logger.debug( "normalized = %d"%occsn )
            tag = " ".join ( tagger.TreeBankPosTagger.getTag( ng['postag'] ) )
            # prepares the row
            row = [ ng['status'], ng['label'], tag,
                    str(occs), str(len(ng['content'])), str(occsn), \
                    " ".join(ng['edges']['Document'].keys()), \
                    " ".join(ng['edges']['Corpus'].keys()), \
                    ng['id']
                ]
            self.writeRow(row)
            # notifies progression
            if ngramcount % 500 == 0:
                TinaApp.notify( None,
                    'tinasoft_runExportCorpora_running_status',
                    str(float( (ngramcount * 100) / ngramtotal ))
                )
            # breaks if limit's exceeded
            if ngramcount > ngramlimit:
                break
        _logger.debug( "Total ngrams exported = %d" % totalexported )
        return self.filepath


    def export_documents( self, storage, periods, corporaid ):
        """OBSOLETE exports corpus' documents"""
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
