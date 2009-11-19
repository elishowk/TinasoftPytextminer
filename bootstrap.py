#!/usr/bin/python
# -*- coding: utf-8 -*-

# setup tina for local environnement (no need to be root, build or to install!)
import sys
try:
    import commands
    arch = "_%s"%commands.getoutput('uname -m')
except:
    arch = ""
libdir = "lib/python-%s.%s_%s%s" % (sys.version_info[0],sys.version_info[1], sys.platform, arch)  
# libdir = "lib/python-2.6_linux2_x86_64"
sys.path = [libdir, 'src'] + sys.path
print "Python environnment initialized:", sys.path

import json
