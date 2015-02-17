#!/usr/bin/python

import sys
import ConfigParser
import plugins.Aid2GiGeneidAccessionUniprot
import plugins.Bioassays
import plugins.Compounds

if len(sys.argv) < 3:
  print "Usage: %s <username> <password>" % __file__
  sys.exit()
user, passwd = sys.argv[1], sys.argv[2]
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

#plugins.Aid2GiGeneidAccessionUniprot.update(user, passwd, cfg.get('default','db'))
plugins.Bioassays.update(user, passwd, cfg.get('default', 'db'))
#plugins.Compounds.update(user, passwd, cfg.get('default', 'db'))