# -*- coding: utf-8 -*-
import tinasoft
from tinasoft.data import Exporter
import datetime

# Tenjin, the fastest template engine in the world !
import tenjin
from tenjin.helpers import *

#print dir(tenjin)

# tinasoft logger
import logging
_logger = logging.getLogger('TinaAppLogger')

# generic GEXF handler
class GEXFHandler (Exporter):

    options = {
        'locale'     : 'en_US.UTF-8',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None,
        'threshold'  : 2,
        'template'   : 'shared/gexf/gexf.template',
    }

    def __init__(self, path, **opts):
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')
        self.engine = tenjin.Engine()

# specific GEXF handler
class Exporter (GEXFHandler):
    """
    Gexf Engine
    """

    def getProximity( self, cooc, occ1, occ2, alpha=0.01 ):
        prox = ( float(cooc) / float(occ1) )**alpha * (float(cooc) / float(occ2))
        #_logger.debug( prox )
        return prox

    def ngramCoocGraph(self, db, periods, threshold=[0,1],\
            meta={}, whitelist=None, degreemax=None):
        """uses Cooc from database to write a cooc graph for a given list of periods"""
        if len(periods) == 0: return
        gexf = {
            'description' : "",
            'creators'    : [],
            'type'        : 'static',
            'attrnodes'   : { 'category' : 'string', 'occ' : 'integer' },
            'attredges'   : { 'cooc' : 'integer' }
        }
        gexf.update(meta)
        i = 1
        nodes = {}
        for period in periods:
            # loads the corpus (=period) object
            corp = db.loadCorpus(period)
            # gets the database cursor for the current period
            generator = db.selectCorpusCooc(period)
            try:
                while 1:
                    ngid1,row = generator.next()
                    if i % 25 == 0: _logger.debug( "%d graph nodes processed"%i )

                    if whitelist is not None and ngid1 not in whitelist:
                        continue
                    # adds the source NGram to the nodes dict
                    if ngid1 not in nodes:
                        # loads the source NGram object
                        ngram1 = db.loadNGram(ngid1)
                        nodes[ngid1] = {
                            #'label' : ngram1["label"],
                            'category' : 'NGram',
                            'weight' : {},
                            'occ' : 0,
                            'cooc' : {},
                            'cache' : ngram1
                        }
                    else:
                        ngram1 = nodes[ngid1]['cache']

                    # gets the source occurrences for the current period
                    if ngid1 in corp['edges']['NGram']:
                        occ1 = nodes[ngid1]['occ'] = corp['edges']['NGram'][ngid1]
                    else:
                        _logger.error("inconsistency found in database,\
                            missing NGram %s edge in Corpus %s"%(ngid1,corp['id']))
                        continue

                    # goes through every target NGram  object
                    for ngid2, cooc in row.iteritems():
                        if whitelist is not None and ngid2 not in whitelist:
                            continue
                        # adds the target NGram to the nodes dict
                        if ngid2 not in nodes:
                            ngram2 = db.loadNGram(ngid2)
                            nodes[ngid2] = { \
                                #'label' : ngram2["label"],
                                'category' : 'NGram',
                                'weight' : {},
                                'occ' : 0,
                                'cooc' : {},
                                'cache' : ngram2
                            }
                        else:
                            ngram2 = nodes[ngid2]['cache']

                        # Sums cooccurences values into nodes dict
                        if ngid2 in nodes[ngid1]['cooc']:
                            nodes[ngid1]['cooc'][ngid2] += cooc
                        else:
                            nodes[ngid1]['cooc'][ngid2] = cooc
                        cooc = nodes[ngid1]['cooc'][ngid2]

                        # gets the target occurrences for the current period
                        if ngid2 in corp['edges']['NGram']:
                            occ2 = nodes[ngid2]['occ'] = corp['edges']['NGram'][ngid2]
                        else:
                            _logger.error("inconsistency found in database,\
                                missing NGram %s edge in Corpus %s"%(ngid1,corp['id']))
                            continue

                        # calculates the nodes proximity
                        # TODO proximity function from config
                        w = self.getProximity( cooc, occ1, occ2 )

                        # filters proximity
                        if threshold[0] <= w and w <= threshold[1]:
                            nodes[ngid1]['weight'][ngid2] = w
                    i+=1
                # FIXME debugging with eg : while i < debuglimit
                #raise StopIteration

            # End of database cursor handler
            except StopIteration:
                # TODO filters N nearest neighbours
                #for nodesource,row in nodes.iteritems():
                #    for nodetarget,cooc in row['cooc'].iteritems():
                #        try:
                #            occ = nodes[nodetarget]['occ']
                #        except:
                #            occ = 0
                #        try:
                #            label = nodes[nodetarget]['label']
                #        except:
                #            label = 0
                #        if occ < cooc:
                #            _logger.error("%s,%s,%s,%s" % (nodetarget,label,cooc,occ))
                # sends data to the templating system
                gexf.update({
                    'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
                    'nodes' : nodes,
                    'threshold' : threshold
                })
            # global exception handler
            except Exception, e:
                import sys,traceback
                traceback.print_exc(file=sys.stdout)
                return tinasoft.TinaApp.STATUS_ERROR
        # renders gexf
        return self.engine.render(self.template,gexf)


    # appellee avec selectCorpusCooc
    #
    def documentCoocGraph(self, db, corpus, threshold=[0,9999999999999999], meta={}):

        gexf = {
            'description' : "",
            'creators'    : [],
            'type'        : 'static',
            'attrnodes'   : { 'category' : 'string', 'occ' : 'integer' },
            'attredges'   : { 'cooc' : 'integer' }
        }
        gexf.update(meta)

        corpusID = str(corpus)

        corp = db.loadCorpus(corpusID)

        generator = db.selectCorpusCooc(corpusID)
        nodes = {}
        occs = {}
        i = 1
        curr = 1
        #_logger.error("ng_id,ng_label,ng_edges_corp_occ,corp_edges_ng_occ,cooc")
        try:
            while i:
                i+=1
                key,row = generator.next()
                if i % 25 == 0: print i

                #print "row:",row
                ngid1,month = key
                ngram1 = db.loadNGram(ngid1)
                if ngid1 not in nodes:
                    nodes[ngid1] = { \
                      'label' : ngram1["label"],
                      'category' : 'NGram',
                      'weight' : {},
                      'occ' : 0,
                      'cooc' : {}
                      }

                if ngid1 in corp['edges']['NGram']:
                    nodes[ngid1]['occ'] = corp['edges']['NGram'][ngid1]
                    occ1 = nodes[ngid1]['occ']

                for ngid2, cooc in row.iteritems():
                    ngram2 = db.loadNGram(ngid2)

                    if ngid2 not in nodes:
                        nodes[ngid2] = { \
                          'label' : ngram2["label"],
                          'category' : 'NGram',
                          'weight' : {},
                          'occ' : 0,
                          'cooc' : {}
                        }

                    if ngid2 in nodes[ngid1]['cooc']:
                        nodes[ngid1]['cooc'][ngid2] += cooc
                    else:
                        nodes[ngid1]['cooc'][ngid2] = cooc

                    cooc = nodes[ngid1]['cooc'][ngid2]

                    if ngid2 in corp['edges']['NGram']:
                        occ2 = nodes[ngid2]['occ'] = corp['edges']['NGram'][ngid2]


                    #if ngid2 not in nodes[ngid1]['distance']:
                    #w = ( cooc/float(ngram['edges']['Corpus'][corpusID]))**0.01*(cooc/float(ngram2['edges']['Corpus'][corpusID]))
                    #if w == 1.0:
                    #    print "ng1 %s : ng2 %s : %s = (%s / %s)**0.01*(%s / %s)" % (ngram['label'],ngram2['label'],w,cooc,float(ngram['edges']['Corpus'][corpusID]),cooc,float(ngram2['edges']['Corpus'][corpusID]))
                    w = ( float(cooc) / float(occ1) )**0.01 * (float(cooc) / float(occ2) )
                    #print "(%s,%s) : %s = (%s / %s)**0.01*(%s / %s)" % (ngram1['label'],ngram2['label'],w,cooc,occs[ngid1],cooc,occs[ngid2])
                    if threshold[0] <= w and w <= threshold[1]:
                        nodes[ngid1]['weight'][ngid2] = w


            raise StopIteration
        except StopIteration:
            _logger.error("ng_id,ng_label,occ,cooc")
            for n1,r1 in nodes.iteritems():
                for n2,cooc in r1['cooc'].iteritems():
                    try:
                        occ = nodes[n2]['occ']
                    except:
                        occ = 0
                    try:
                        label = nodes[n2]['label']
                    except:
                        label = 0
                    if occ < cooc:
                        _logger.error("%s,%s,%s,%s" % (n2,label,cooc,occ))

            #if occ1 < cooc:
            #    #err = "(%s,!%s) : %s = (%s / %s)**0.01*(%s / %s)" % (ngram1['label'],ngram2['label'],w,cooc,occs[ngid1],cooc,occs[ngid2])
            #    err = "%s,%s,%s,%s,%s" % (ngid1, ngram1['label'],ngram1['edges']['Corpus'][corpusID],occ1,cooc)
            #    #occs[ngid1] = cooc
            #    _logger.error(err)

            #if occ2 < cooc:
            #    err = "%s,%s,%s,%s,%s" % (ngid2, ngram2['label'],ngram2['edges']['Corpus'][corpusID],occ2,cooc)
            #    #occs[ngid2] = cooc
            #    _logger.error(err)

            gexf.update({
                        'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
                        'nodes' : nodes,
                        'threshold' : threshold
            })
            return self.engine.render(self.template,gexf)
        except Exception, e:
            import sys,traceback
            traceback.print_exc(file=sys.stdout)

        return self.engine.render({})
