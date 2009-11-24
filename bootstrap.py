#!/usr/bin/python
# -*- coding: utf-8 -*-
try:
    import commands
    arch = "_%s"%commands.getoutput('uname -m')
except:
    arch = ""
import sys

sys.path = [ 
   "lib/python-%s.%s_%s%s" % (sys.version_info[0],sys.version_info[1], sys.platform, arch), # C MODULES
   "lib/python-%s.%s" % (sys.version_info[0],sys.version_info[1]), # PYTHON MODULES
    'src'] + sys.path # PROJECT MODULES, AND DEFAULT SYSTEM MODULES 
print sys.path

try:
    import nltk
    import yaml
    import jsonpickle
    import pickle
    import os
    import sys
except Exception, exc:
    print "Dependency not found:", exc
# import nltk data
import nltk
nltk.data.path = ['shared/nltk_data']

# disable warnings
import warnings 
warnings.filterwarnings("ignore") 
