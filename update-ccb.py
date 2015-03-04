#!/usr/bin/python

import sys
"""
mysq.connector is only installed on the acf cluster for python2.6 but we need
to use python2.7+ because the 'with' directive is not backwards compatible
with v2.6.*
"""
import socket
import getopt
if socket.gethostname()[-6:] == "ku.edu":
	sys.path.append('/usr/lib/python2.6/site-packages/')
import ConfigParser
import plugins.Aid2GiGeneidAccessionUniprot
import plugins.Assay_id_assay_description
import plugins.Bioassays
import subprocess

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

def help():
  print "---------------------------------------------------------"
  print "%s - The %s database update script" % (__file__, cfg.get('default', 'database'))
  print "---------------------------------------------------------"
  print "* For information, see %s." % (cfg.get('repo', 'url'))
  print "* Report issues at %s or " % cfg.get('repo', 'issues')
  print "* contact %s at <%s>" % (cfg.get('author', 'name'), cfg.get('author', 'email'))
  print "Usage: %s <username> <password>" % __file__
  print ""

shortargs = "hH:u:p:"
longargs  = ["help","hostname=","username=","password="]

# Read command line options
opts, args = getopt.getopt(sys.argv[1:], shortargs, longargs)
hostname, username, password, database = None,None,None,cfg.get('default','database')
print opts, args
for option, value in opts:
  if option in ("-h", "--help"):
    help();
    sys.exit();
  elif option in ("-u", "--username"):
    username = value
  elif option in ("-p", "--password"):
    password = value
  elif option in ("-H", "--hostname"):
    hostname = value
  else:
    assert False, "unhandled option"

print "Connecting to %s with username %s..." % (hostname, username)

# Create database backup with mysqldump

# Run table update scripts
plugins.Assay_id_assay_description.update(username, password, database, hostname);
plugins.Aid2GiGeneidAccessionUniprot.update(username, password, database, hostname);
#plugins.Bioassays.update(username, password, database, hostname);

