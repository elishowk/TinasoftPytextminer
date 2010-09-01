# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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

__author__="elishowk@nonutc.fr"
# used for FS handling
import os

from tinasoft.data import basecsv, Handler
from tinasoft.pytextminer import ngram, corpus, stemmer, filtering

from decimal import *

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
        ("maxperiodoccs", "max occs per period"),
        ("maxperiod", "max occs period id"),
        ("maxperiodoccsn", "max occs  ^ length per period"),
        ("maxperiodn", "max occs ^ length period id"),
        ("forms", "ngram forms"),
        ("corplist", "corp list"),
        ("dbid", "db ID")
    ]
    accept = "w"
    refuse = "s"
    forms_separator = " %%% "

    def __init__(self):
        return



class Importer(basecsv.Importer):
    """A class for csv imports of selected ngrams whitelists"""
    storage = None
    filemodel = WhitelistFile()
    whitelist = None

    def _add_whitelist(self, dbid, occs):
        """
        adds a whitelisted ngram
        """
        self.whitelist.addEdge( 'NGram', dbid, occs )

    def _add_stopword(self, dbid, occs):
        """
        adds a user defined stop-ngram
        """
        self.whitelist.addEdge( 'StopNGram', dbid, occs )

    def _add_ngram(self, dbid, ngobj):
        """
        General method adding any ngram to the whitelist
        """
        self.whitelist['content'][dbid] = ngobj

    def parse_file(self):
        """Reads a whitelist file and returns the updated object"""
        stem = stemmer.Nltk()
        if self.whitelist is None: return False
        for row in self.csv:
            try:
                status = row[self.filemodel.columns[0][1]]
                occs = int(row[self.filemodel.columns[3][1]])
                label = row[self.filemodel.columns[1][1]]
                forms_tokens = row[self.filemodel.columns[10][1]].split(self.filemodel.forms_separator)
                # prepares forms ID to add them to the whitelist edges
                forms_id = [ngram.NGram.getId(tokens.split(" ")) for tokens in forms_tokens]
                # prepares forms label to add the to NGram objects in the whitelist
                forms_label = dict().fromkeys( [tokens for tokens in forms_tokens] , 1 )
                periods = row[self.filemodel.columns[11][1]].split(self.filemodel.forms_separator)
            except KeyError, keyexc:
                _logger.error( "%s columns was not found, whitelist import failed"%keyexc )
                continue
            # instanciate a NGram object
            edges = { 'Document' : {}, 'Corpus' : {}, 'label': forms_label, 'postag' : {}}
            ng = ngram.NGram(label.split(" "), stemmer=stem, edges=edges)
            # calculates db ID from the label, does not trust the file's db id
            for corpid in periods:
                # increments all the Corpus edges
                self.whitelist.addEdge( 'Corpus', str(corpid), 1 )
                ng.addEdge( 'Corpus', str(corpid), 1)
                # keeps a corpus object into whitelist object
                if corpid not in self.whitelist['corpus']:
                    self.whitelist['corpus'][corpid] = corpus.Corpus(corpid)
            dbid = ng['id']
            # increments the whitelist edges
            if status == self.filemodel.accept:
                # adds all forms to the whitelist
                for form_id in forms_id:
                    self._add_whitelist(dbid, 0)
                self._add_whitelist(dbid, occs)
                self._add_ngram(dbid, ng)
            elif status == self.filemodel.refuse:
                self._add_stopword(dbid, occs)
        return self.whitelist

def load_from_storage(whitelist, storage, periods, filters=None, wlinstance=None):
    """
    TODO move to export_whitelist() utility in data.whitelist
    Whitelist creator/updater utility
    Loads a whitelist from storage
    """
    filemodel = WhitelistFile()
    for corpusid in periods:
        # gets a corpus from the storage or continue
        corpusobj = storage.loadCorpus(corpusid)
        if corpusobj is None:
            _logger.error( "corpus %s not found"%corpusid )
            continue
        # increments number of docs per period
        if  corpusid not in periods:
            whitelist['corpus'][corpusid] = corpus.Corpus(corpusid, edges=corpusobj['edges'])

        # TODO sorts ngrams by occs
        #sortedngrams = reversed(sorted(corpusobj['edges']['NGram'].items(), key=itemgetter(1)))
        # walks through ngrams in the corpus
        # occ is the number of docs in the corpus where ngid app
        for ngid, occ in corpusobj['edges']['NGram'].iteritems():
            # if NGram's unknown, then loads an checks ngram
            ng = storage.loadNGram(ngid)
            if ng is None:
                _logger.error( "ngram not found %s in database %s"%(ngid,corpusid) )
                continue
            # default status
            ng['status'] = ''
            # if a complementary whitelist is given
            if wlinstance is not None:
                # increment edge weight by status
                if ngid in wlinstance['edges']['StopNGram']:
                    ng['status'] = filemodel.refuse
                if ngid in wlinstance['edges']['NGram']:
                    ng['status'] = filemodel.accept
            # if filtering is active
            if filters is not None and filtering.apply_filters(ng, filters) is False:
                ng['status'] = filemodel.refuse
            # updates whitelist's edges
            if ng['status'] == filemodel.refuse:
                whitelist.addEdge('StopNGram', ngid, occ)
            else:
                whitelist.addEdge( 'NGram', ngid, occ )

            # add ngram to cache or update the status
            if ngid not in  whitelist['content']:
                whitelist['content'][ngid] = ng
            else:
                whitelist['content'][ngid] = ng['status']
        return whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams Whitelists"""

    filemodel = WhitelistFile()

    def export_whitelist(self, storage, periods, newwl, filters=None, compl_whitelist=None, minOccs=1):
        """
        Used only to export index NGrams from storage
        creates a Whitelist Object given periods and optional filters, and another whitelist object to be merged
        """
        # loads whitelist object from storage
        whitelist = load_from_storage(newwl, storage, periods, filters, compl_whitelist)
        # exports to a file
        (export_path, whitelist) = self.write_whitelist(whitelist, minOccs)
        # cleans whitelist
        whitelist['content'] = whitelist['corpus'] = {}
        # updates whitelist into storage
        storage.insertWhitelist(whitelist, self.whitelist['id'], overwrite=True)
        return export_path

    def write_whitelist(self, newwl, minOccs=1):
        """
        Writes a Whitelist object to a file
        """
        self.writeRow([x[1] for x in self.filemodel.columns])
        # basic monitoring counters
        totalexported = 0
        ngramtotal = len(newwl['content'].keys())
        _logger.debug( "Writing %d ngrams to whitelist at %s" % (ngramtotal, self.filepath) )

        for ngid in newwl['content'].keys():
            ng =  newwl['content'][ngid]
            ngid = str(ngid)
            # filters ngram from the whitelist based on min occs
            if ngid in newwl['edges']['StopNGram']:
                occs = newwl['edges']['StopNGram'][ngid]
                ng['status'] = self.filemodel.refuse
                if not occs >= minOccs:
                    del newwl['edges']['StopNGram'][ngid]
                    del newwl['content'][ngid]
                    continue
            elif ngid in newwl['edges']['NGram']:
                occs = newwl['edges']['NGram'][ngid]
                if not occs >= minOccs:
                    del newwl['edges']['NGram'][ngid]
                    del newwl['content'][ngid]
                    continue
            totalexported += 1
            occsn = occs**len(ng['content'])
            # TODO update NGram in db after adding new scores
            #if 'MaxCorpus' not in ng['edges'] or 'MaxNormalizedCorpus' not in ng['edges']:
            maxperiod = maxnormalizedperiod = lastmax = lastnormmax = Decimal(0)
            maxperiodid = maxnormalizedperiodid = None
            for periodid, totalperiod in ng['edges']['Corpus'].iteritems():
                if periodid not in newwl['corpus']: continue
                totaldocs =  len(newwl['corpus'][periodid]['edges']['Document'].keys())
                if totaldocs == 0: continue
                # updates both per period max occs
                lastmax = Decimal(totalperiod) / Decimal(totaldocs)

                if lastmax >= maxperiod:
                    maxperiod = lastmax
                    maxperiodid = periodid

                lastnormmax = Decimal(totalperiod**len(ng['content'])) / Decimal(totaldocs)

                if lastnormmax >= maxnormalizedperiod:
                    maxnormalizedperiod = lastnormmax
                    maxnormalizedperiodid = periodid
            # stores meta informations
            ng.addEdge('MaxCorpus',maxperiodid,maxperiod)
            ng.addEdge('MaxNormalizedCorpus',maxnormalizedperiodid,maxnormalizedperiod)

            # gets major forms
            label = ng.getLabel().replace('"',"'")
            tag = ng.getPostag()
            # get all forms and appropriate list of corpus to export
            forms = self.filemodel.forms_separator.join(ng['edges']['label'].keys())
            corp_list = self.filemodel.forms_separator.join([corpid for corpid in ng['edges']['Corpus'].keys() if corpid in newwl['corpus']])
            # prepares the row
            row = [
                ng['status'],
                label,
                tag,
                occs,
                len(ng['content']),
                occsn,
                maxperiod,
                str(maxperiodid),
                maxnormalizedperiod,
                str(maxnormalizedperiodid),
                forms,
                corp_list,
                str(ngid)
            ]
            print row
            self.writeRow(row)
            # TODO restore when whitelist stored in DB updates ng in whitelist object
            #newwl['content'][ngid] = ng
            # TEMPORARY clean memory usage
            del newwl['content'][ngid]
            # notifies progression
            #if ngramcount % 500 == 0:
            #    TinaApp.notify( None,
            #        'tinasoft_runExportCorpora_running_status',
            #        str(float( (ngramcount * 100) / ngramtotal ))
            #    )
        _logger.debug( "%d ngrams exported after filtering" %totalexported )
        return (self.filepath, newwl)


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
