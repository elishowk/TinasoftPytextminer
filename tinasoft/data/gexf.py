# -*- coding: utf-8 -*-
from tinasoft.data import Handler
import datetime

import tenjin
from tenjin.helpers import *   # or escape, to_str

engine = tenjin.Engine()

def render(**model):
    return engine.render('gexf.template', model)

class Model:
    def __init__(self, description='A test GEXF'):
        self.date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.description = description
        self.type = 'static'
        self.creators = ['Julian bilcke', 'Elias Showk']
        self.attr_nodes = { 'category' : 'string' }
        self.attr_edges = { 'category' : 'string' }
            
        self.nodes = []
        self.edges = []
        
# generic GEXF handler
class GEXFHandler (Handler):

    options = {
        'locale'     : 'en_US.UTF-8',
        'dieOnError' : False,
        'debug'      : False,
        'compression': None
    }
    
    def __init__(self, path, **opts):
        self.path = path
        self.loadOptions(opts)
        self.lang,self.encoding = self.locale.split('.')

# specific GEXF handler
class Engine (GEXFHandler):
    """
    Gexf Engine
    """
    def save(self, path):
        model = Model("test gexf")
        render(model)

