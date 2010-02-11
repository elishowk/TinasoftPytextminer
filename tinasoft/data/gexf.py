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
 
    # appellee avec selectCoprusCooc
    # 
    def coocGraph(self, cursor, threshold=0):
        nodes = {}
        try:
            while 1:
                key,row = cursor.next()
                #print "row:",row
                id,month = key
                if id not in nodes:
                    nodes[id] = { 'label' : id, 
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
            'threshold' : threshold
        })
        except Exception, e:
            print "Exception:",e
        return render({})
