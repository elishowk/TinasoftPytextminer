"""
This is a setup.py script generated by py2applet

Usage:
    python setup.py py2app
"""
import os
from os.path import join
from glob import glob

from setuptools import setup

APP = ['Tinasoft.py']
DATA_FILES = [
    #('',glob(r'desktop_config_unix.yaml')),
    #('',glob(r'LICENSE')),
    #('',glob(r'README')),
    #('source_files',glob(join('source_files','*.csv'))),
    #(join('shared','gexf'), glob(join('shared','gexf','gexf.template'))),
    #(join('shared','stopwords'), glob(join('shared','stopwords','*'))),
    #(join('shared','nltk_data','corpora','brown'), glob(join('shared','nltk_data','corpora','brown','*'))),
    #(join('shared','nltk_data','corpora','conll2000'), glob(join('shared','nltk_data','corpora','conll2000','*'))),
    #(join('shared','nltk_data','tokenizers','punkt'), glob(join('shared','nltk_data','tokenizers','punkt','*')))
]

OPTIONS = {
	'argv_emulation': True,
	'argv_inject': "config_unix.yaml",
	#'packages': ['nltk','numpy','twisted','twisted.web','twisted.internet','encodings','zope.interface'],
	'includes': ['tinasoft.data.gexf','tinasoft.data.medline','tinasoft.data.tinasqlite','tinasoft.data.tinacsv','tinasoft.data.whitelist','tinasoft.data.coocmatrix','tinasoft.data.basecsv','traceback','zope.interface','twisted','nltk','numpy','jsonpickle','yaml','tenjin','simplejson','twisted.web','twisted.internet','encodings'],
	'excludes': ['_gtkagg', '_tkagg', 'curses', 'email', 'tcl',
            'Tkconstants', 'Tkinter','tinasoft.data.tinabsddb','bsddb3'],
	#'resources': DATA_FILES,
	'plist': {
	    'LSEnvironment': {'NLTK_DATA':'shared/nltk_data'}
	},
}
setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)

