#!/usr/bin/python
# -*- coding: utf-8 -*-

# disable warnings for nltk and dependency overwrite

try:
    import commands
    arch = "_%s"%commands.getoutput('uname -m')
except:
    arch = ""
import sys

sys.path += [ # try to get libraries installed in the system first
    'src',
   "lib/python-%s.%s_%s%s" % (
          sys.version_info[0],sys.version_info[1], 
          sys.platform, 
          arch),
   "lib/universal" ] # in last resort, we use the packaged pure-python libraries (but they are slow)


import warnings 
warnings.filterwarnings("ignore") 
import nltk
nltk.data.path = ['shared/nltk_data']


