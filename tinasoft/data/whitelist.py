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

from tinasoft.data import basecsv
from tinasoft.data import Importer as BaseImporter
from tinasoft.pytextminer import ngram
# tinasoft's logger
import logging
_logger = logging.getLogger('TinaAppLogger')


class WhitelistFile(object):
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
        ("periods", "periods"),
        ("documents", "documents")
    ]
    accept = "w"
    refuse = "s"
    forms_separator = "***"


class Importer(basecsv.Importer, BaseImporter):
    """A class for csv imports of selected ngrams whitelists"""
    storage = None
    filemodel = WhitelistFile()
    whitelist = None

    def __init__(self, path, **kwargs):
        BaseImporter.__init__(self, path, **kwargs)
        basecsv.Importer.__init__(self, path, **kwargs)

    def parse_file(self):
        """Reads a whitelist file and returns a whitelist object"""
        if self.whitelist is None: return False
        ngramqueue=[]
        for row in self:
            if row is None: continue
            
            try:
                status = self._coerce_unicode( row[self.filemodel.columns[0][1]] ).strip()
                ### breaks if not whitelisted
                if status != self.filemodel.accept: continue
                label = self._coerce_unicode( row[self.filemodel.columns[1][1]] ).strip()
                forms_labels = [self._coerce_unicode( form ).strip() for form in row[self.filemodel.columns[4][1]].split( self.filemodel.forms_separator )]
            except KeyError, keyexc:
                _logger.error( "%s column (required) not found importing the whitelist at line %d, import failed"%(keyexc, self.reader.line_num) )
                continue
                
            # main NGram
            ngobj = ngram.NGram(label.split(" "), label=label)
            ngid = ngobj['id']
            # restores all forms
            for form in forms_labels:
                formtokens = form.split(" ")
                formobj = ngram.NGram(formtokens)
                self.whitelist.addEdge( "NGram", formobj['id'], ngid )
                ngobj.addForm(formtokens)
            # stores the NGram into whitelist storage
            #self.whitelist.addNGram( ngobj )
            ngramqueue += [(ngobj['id'], ngobj)]
            # these edges are used to cache labels, see tokenizer
            self.whitelist.addEdge( 'form_label', ngid, label )
            
        self.file.close()
        ### insert/update document
        #self.storage.flushNGramQueue()
        self.whitelist.storage.insertManyNGram( ngramqueue )
        #self.whitelist.storage.flushNGramQueue()
        return self.whitelist

class Exporter(basecsv.Exporter):
    """A class for csv exports of NGrams Whitelists"""

    filemodel = WhitelistFile()

    def write_whitelist(self, newwl, corporaId, minoccs=1):
        """
        Writes a Whitelist object to a file
        """
        self.writeRow([x[1] for x in self.filemodel.columns])
        totalexported = 0

        corpusCache = {}
        corporaObj = newwl.storage.loadCorpora(corporaId)
        if corporaObj is None:
            raise Exception("corpora %s not found, impossible to export whitelist"%newwl.label)
            return
        
        for corpusId in corporaObj['edges']['Corpus'].iterkeys():
            corpusCache[corpusId] = newwl.storage.loadCorpus(corpusId)

        # cursor of Whitelist NGrams db
        ngramgenerator = newwl.getNGram()
        try:
            while 1:
                ngid, ng = ngramgenerator.next()
                
                # sums all NGram-Corpus occurrences to get total occs
                occs = sum( ng['edges']['Corpus'].values() )
                #_logger.debug("sum = %d"%occs)
                # filters ngram by total occurrences
                if occs < minoccs:
                    #print "sum = %d"%occs
                    #print ng['edges']['Corpus'].keys()
                    continue

                ng.updateMajorForm()
                ng['status'] = ""

                # prepares some score values
                occsn = occs**len(ng['content'])
                maxperiod = maxnormalizedperiod = lastmax = lastnormmax = 0.0
                maxperiodid = maxnormalizedperiodid = None

                for periodid, totalperiod in ng['edges']['Corpus'].iteritems():
                    totaldocs =  len(corpusCache[periodid]['edges']['Document'].keys())
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
                        
                separator = " "+self.filemodel.forms_separator+" "

                forms = separator.join( ng['edges']['label'].keys() )
                corp_list = separator.join(
                    [corpid for corpid in ng['edges']['Corpus'].keys()]
                )
                doc_list = separator.join(
                    [docid for docid in ng['edges']['Document'].keys()]
                )
                
                row = [
                    unicode(ng['status']),
                    unicode(ng.label),
                    unicode(ng.postag),
                    int(occs),
                    unicode(forms),
                    len(ng['content']),
                    int(occsn),
                    float(maxperiod),
                    unicode(maxperiodid),
                    float(maxnormalizedperiod),
                    unicode(maxnormalizedperiodid),
                    unicode(corp_list),
                    unicode(doc_list)
                ]
                totalexported += 1
                self.writeRow(row)

        except StopIteration:
            _logger.debug( "%d ngrams exported after filtering" %totalexported )
            self.file.close()
            return self.filepath