#!/usr/bin/python
# -*- coding: utf-8 -*-

# disable warnings for nltk and dependency overwrite

try:
    import commands
    arch = "_%s"%commands.getoutput('uname -m')
except:
    arch = ""
import sys


sys.path = ['src'] + [
   "lib/python-%s.%s-%s%s" % (sys.version_info[0],sys.version_info[1], sys.platform, arch), 
   "lib/universal" ] + sys.path 
#print sys.path   
import warnings 
warnings.filterwarnings("ignore") 
import nltk
nltk.data.path = ['shared/nltk_data']


