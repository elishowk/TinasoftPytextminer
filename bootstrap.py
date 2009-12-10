#!/usr/bin/python
# -*- coding: utf-8 -*-
# disable warnings for nltk and dependency overwrite
# updates sys.path including locally built modules
try:
    import commands
    arch = "_%s"%commands.getoutput('uname -m')
except:
    arch = ""
import sys

sys.path = ['src'] + [ "lib/universal" ] + sys.path 

#sys.path = [ "src/lib/python" ] + sys.path
#print sys.path
import warnings 
warnings.filterwarnings("ignore") 
import nltk
nltk.data.path = ['shared/nltk_data']
