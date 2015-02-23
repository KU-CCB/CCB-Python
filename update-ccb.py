#!/usr/bin/python

import sys
"""
mysq.connector is only installed on the acf cluster for python2.6 but we need
to use python2.7+ because the 'with' directive is not backwards compatible
with v2.6.*
"""
import socket
if socket.gethostname()[-6:] == "ku.edu":
	sys.path.append('/usr/lib/python2.6/site-packages/')
import ConfigParser
import plugins.Aid2GiGeneidAccessionUniprot
import plugins.Bioassays
import plugins.Compounds

if len(sys.argv) < 3:
  print "CCBDB.update-ccb  The KU CCB Database update script"
  print "  For more information, see https://github.com/KU-CCB/CCBDB.git"
  print ""
  print "  For issues see https://github.com/KU-CCB/CCBDB/issues or"
  print "  contact Kendal Harland <kendaljharland@gmail.com>"
  print ""
  print "  Usage: %s <username> <password>" % __file__
  sys.exit()
user, passwd = sys.argv[1], sys.argv[2]
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

plugins.Aid2GiGeneidAccessionUniprot.update(user, passwd, cfg.get('default','database'))
plugins.Bioassays.update(user, passwd, cfg.get('default', 'database'))
plugins.Compounds.update(user, passwd, cfg.get('default', 'database'))
