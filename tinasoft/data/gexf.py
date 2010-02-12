# -*- coding: utf-8 -*-
from tinasoft.data import Handler
import datetime

import tenjin
from tenjin.helpers import *   # or escape, to_str
import logging


_logger = logging.getLogger('TinaAppLogger')
engine = tenjin.Engine()


# generic GEXF handler
class GEXFHandler (Handler):

    options = {
        'locale'     : 'en_US.UTF-8',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None,
        'threshold'  : 2,
    }
    
    def __init__(self, path, **opts):
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')

# specific GEXF handler
class Exporter (GEXFHandler):
    """
    Gexf Engine
    """
 
    # appellee avec selectCorpusCooc
    # 
    def coocGraph(self, db, corpusID, min=0, max=999999999):
        generator = db.selectCorpusCooc(corpusID)
        nodes = {}
        try:
            while 1:
                key,row = generator.next()
                #print "row:",row
                id,month = key
                if id not in nodes:
                    nodes[id] = { 'label' : db.loadNGram(id)["label"], 
                                        #'x' : 0, 
                                        #'y' : 0,
                                        'category' : 'NGram',
                                         'edges' : {} }
                
                
                for ngram, cooc in row.iteritems():
                    if ngram in nodes[id]['edges']:
                        nodes[id]['edges'][ngram] += cooc
                    else:
                        nodes[id]['edges'][ngram] = cooc

        except StopIteration:
            return engine.render('tinasoft/data/gexf.template',{
                        'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
                        'description' : "test cooc graph",
                        'type' : 'static',
                        'creators' : ['Julian bilcke', 'Elias Showk'],
                        'attrnodes' : { 'category' : 'string' },
                        'attredges' : { 'category' : 'string' },
                        'nodes' : nodes,
                        'min' : min,
                        'max' : max
            })
        except Exception, e:
            print "Exception:",e
        return engine.render({})
        
        
        
        # appellee avec selectCorpusCooc
    # 
    def coocDistanceGraph(self, db, corpus, threshold=[0,9999999999999999], meta={}):
        
        gexf = {
            'description' : "",
            'creators'    : [],
            'type'        : 'static',
            'attrnodes'   : { 'category' : 'string' },
            'attredges'   : { 'category' : 'string' }
        }
        gexf.update(meta)
        
        corpusID = str(corpus)
        generator = db.selectCorpusCooc(corpusID)
        nodes = {}
        i = 1
        try:
            while i < 50:
                i+=1
                key,row = generator.next()
                #print "row:",row
                id,month = key
                ngram = db.loadNGram(id)
                if id not in nodes:
                    nodes[id] = { \
                      'label' : ngram["label"], 
                      'category' : 'NGram',
                      'distance' : {},
                      'cooc'     : {} }
                                         
                for ng, cooc in row.iteritems():
                    ngram2 = db.loadNGram(ng) 
                    cooc = float(cooc)
                    if ng in nodes[id]['cooc']:
                        nodes[id]['cooc'][ng] += cooc
                    else:
                        nodes[id]['cooc'][ng] = cooc
                        
                    if ng not in nodes[id]['distance']:
                        w = ( cooc/float(ngram['edges']['Corpus'][corpusID]))**0.01*(cooc/float(ngram2['edges']['Corpus'][corpusID]))
                        if threshold[0] <= w and w <= threshold[1]:
                             nodes[id]['distance'][ng] = w
                            
                        
            raise StopIteration
        except StopIteration:
            gexf.update({
                        'date' : "%s"%datetime.datetime.now().strftime("%Y-%m-%d"),
                        'nodes' : nodes,
                        'threshold' : threshold
            })
            return engine.render('tinasoft/data/gexf.template',gexf)
        except Exception, e:
            print "Exception:",e
        return engine.render({})
