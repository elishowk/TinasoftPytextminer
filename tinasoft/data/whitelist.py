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

__author__ = "elishowk@nonutc.fr"

from tinasoft.data import basecsv, Handler
from tinasoft.pytextminer import ngram, corpus, stemmer, filtering
# tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class WhitelistFile():
    """
    Model of a whitelist csv file
    """
    columns = [
        ("status", "status"),
        ("label", "label"),
        ("postag", "pos tag"),
        ("occs", "total occs"),
        ("forms", "ngram forms"),
        ("length", "length"),
        ("occsn", "total occs ^ length"),
        ("maxperiodoccs", "max occs per period"),
        ("maxperiod", "max occs period id"),
        ("maxperiodoccsn", "max occs  ^ length per period"),
        ("maxperiodn", "max occs ^ length period id"),
        ("corplist", "period list"),
        ("dbid", "db ID")
    ]
    accept = "w"
    refuse = "s"
    forms_separator = " *** "

    def __init__(self):
        return


class Importer(basecsv.Importer):
    """A class for csv imports of selected ngrams whitelists"""
    storage = None
    filemodel = WhitelistFile()
    whitelist = None

    def _add_whitelist(self, ngid, occs):
        """
        adds a whitelisted ngram
        """
        self.whitelist.addEdge( 'NGram', ngid, occs )

    def _add_stopword(self, dbid, occs):
        """
        adds a user defined stop-ngram
        """
        self.whitelist.addEdge( 'StopNGram', dbid, occs )

    def _coerce_unicode(self, cell):
        """
        checks a value and eventually convert to type
        """
        #if type(cell) == int or type(cell) == float:
        #    cell = str(cell)
        if type(cell) != unicode:
            return unicode(cell, "utf-8", errors='replace')
        else:
            return cell

    def parse_file(self, stem):
        """Reads a whitelist file and returns a whitelist object"""
        if self.whitelist is None: return False
        for row in self:
            if row is None: continue
            try:
                status = self._coerce_unicode( row[self.filemodel.columns[0][1]] )
                if status != self.filemodel.accept: continue
                label = self._coerce_unicode( row[self.filemodel.columns[1][1]] )
                # gets forms tokens
                forms_tokens = dict().fromkeys(
                    self._coerce_unicode( row[self.filemodel.columns[4][1]] ).split( self.filemodel.forms_separator ),
                    1
                )
                # prepares forms ID to add them to the whitelist edges
                forms_id = [ngram.NGram.getNormId(tokens.split(" ")) for tokens in forms_tokens.iterkeys()]
                periods = self._coerce_unicode( row[self.filemodel.columns[11][1]] ).split( self.filemodel.forms_separator )
            except KeyError, keyexc:
                _logger.error( "%s column (required) not found importing the whitelist at line %d, import failed"%(keyexc, self.reader.line_num) )
                continue
            # prepares and stores a new NGram object
            edges = { 'label': forms_tokens, 'postag' : {}}
            stemmedtokens = [stem.stem(token) for token in label.split(" ")]
            ng = ngram.NGram(stemmedtokens, label=label, edges=edges)
            self.whitelist.addContent(ng)
            # links periods
            for corpid in periods:
                # increments all edges with the corpus
                self.whitelist.addEdge( 'Corpus', corpid, 1 )
                ng.addEdge( 'Corpus', corpid, 1)
                # keeps a corpus object into whitelist object
                if corpid not in self.whitelist['corpus']:
                    self.whitelist['corpus'][corpid] = corpus.Corpus(corpid)
            # adds all forms to the whitelist
            for form_id in forms_id:
                self._add_whitelist( form_id, 1 )
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
        totalexported = 0
        ngramgenerator = newwl.getContent()
        try:
            while 1:
                ngid, ng = ngramgenerator.next()
                # filters ngram from the whitelist based on min occs
                occs=0
                if ngid in newwl['edges']['NGram']:
                    # empty the status columns before exporting to the file
                    ng['status'] = ""
                    occs = newwl['edges']['NGram'][ngid]
                    if not occs >= minOccs: continue
                occsn = occs**len(ng['content'])
                maxperiod = maxnormalizedperiod = lastmax = lastnormmax = 0.0
                maxperiodid = maxnormalizedperiodid = None
                for periodid, totalperiod in ng['edges']['Corpus'].iteritems():
                    if periodid not in newwl['corpus']: continue
                    totaldocs =  len(newwl['corpus'][periodid]['edges']['Document'].keys())
                    if totaldocs == 0: continue
                    # updates both per period max occs
                    lastmax = float(totalperiod) / float(totaldocs)

                    if lastmax >= maxperiod:
                        maxperiod = lastmax
                        maxperiodid = periodid

                    lastnormmax = float(totalperiod**len(ng['content'])) / float(totaldocs)

                    if lastnormmax >= maxnormalizedperiod:
                        maxnormalizedperiod = lastnormmax
                        maxnormalizedperiodid = periodid

                # gets major forms
                label = ng.getLabel()
                tag = ng.getPostag()
                # get all forms and appropriate list of corpus to export
                forms = self.filemodel.forms_separator.join(
                    ng['edges']['label'].keys()
                )
                corp_list = self.filemodel.forms_separator.join(
                    [corpid for corpid in ng['edges']['Corpus'].keys()]
                )
                # prepares the row
                row = [
                    unicode(ng['status']),
                    unicode(label),
                    unicode(tag),
                    int(occs),
                    unicode(forms),
                    len(ng['content']),
                    int(occsn),
                    float(maxperiod),
                    unicode(maxperiodid),
                    float(maxnormalizedperiod),
                    unicode(maxnormalizedperiodid),
                    unicode(corp_list),
                    unicode(ngid)
                ]
                totalexported += 1
                self.writeRow(row)

        except StopIteration, si:
            _logger.debug( "%d ngrams exported after filtering" %totalexported )
            return (self.filepath, newwl)
