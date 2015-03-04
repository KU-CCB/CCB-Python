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
import plugins.Assay_id_assay_description
import plugins.Bioassays
import subprocess

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

if len(sys.argv) < 3:
  print "---------------------------------------------------------"
  print "%s - The %s database update script" % (__file__, cfg.get('default', 'database'))
  print "---------------------------------------------------------"
  print "* For information, see %s." % (cfg.get('repo', 'url'))
  print "* Report issues at %s or " % cfg.get('repo', 'issues')
  print "* contact %s at <%s>" % (cfg.get('author', 'name'), cfg.get('author', 'email'))
  print "Usage: %s <username> <password>" % __file__
  print ""
  sys.exit()
user, passwd = sys.argv[1], sys.argv[2]

# Create database backup with mysqldump

# Run table update scripts
plugins.Aid2GiGeneidAccessionUniprot.update(user, passwd, cfg.get('default','database'))
plugins.Bioassays.update(user, passwd, cfg.get('default', 'database'))
plugins.Assay_id_assay_description.update(user, passwd, cfg.get('default', 'database'))