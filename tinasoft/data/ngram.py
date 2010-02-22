# -*- coding: utf-8 -*-

from tinasoft.data import basecsv
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
        'accept':'x',
        'refuse':'s',
        'whitelist':{},
        'stopwords':{},
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
    def exportNGrams(self): pass

    def exportCorpora(self): pass

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

    # obsolete
    def exportCorpusNGram(self, corpus, filepath, **kwargs):
        """export a file containing a corpus' NGrams"""
        def printpostag(record):
            """prepares the postag field printing"""
            postag = ""
            if record[1]['postag'] is not None:
                for word in record[1]['postag']:
                    postag += "_".join([word[1],word[0]]) + ","
            return postag

        gen = self.storage.select('NGram')
        csv = Writer('basecsv://'+filepath, **kwargs)
        try:
            record = gen.next()
            while record:
                if corpus['id'] in record[1]['edges']['Corpus']:
                    #postag = printpostag(record)
                    #occs = corpus['edges']['Ngram'][record[1]['id']]
                    csv.writeRow( [record[1]['id'], record[1]['label'] ])
                record = gen.next()
        except StopIteration, si: return

    # obsolete
    def getAllCorpus(self, raw=True):
        """fix decode/encode non optimal process"""
        corpuslst = []
        gen = self.storage.select('Corpus')
        try:
            record = gen.next()
            while record:
                corpuslst += [record[1]]
                record = gen.next()
        except StopIteration, si:
            if raw is True:
                return corpuslst
            else:
                return self.storage.encode( corpuslst )
