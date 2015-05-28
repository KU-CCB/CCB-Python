#!/usr/bin/python

import sys
import subprocess
import socket
import getopt
"""
mysq.connector is only installed on the acf cluster for python2.6 but we need
to use python2.7+ because the 'with' directive is not backwards compatible
with v2.6.*
"""
if socket.gethostname()[-6:] == "ku.edu":
	sys.path.append('/usr/lib/python2.6/site-packages/')
import ConfigParser
import plugins.Substances
import plugins.Assay2Gene
import plugins.Activities
#import plugins.Assays

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

def help():
  print ""
  print "Usage: python %s [-H|--hostname=127.0.0.1] [-u|--username] [-p|--password]" % __file__
  print ""

# Read command line options
shortargs = "hH:u:p:"
longargs  = ["help","hostname=","username=","password="]
opts, args = getopt.getopt(sys.argv[1:], shortargs, longargs)
hostname, username, password, database = "127.0.0.1", None, None, cfg.get('default','database')

if len(opts) == 0:
  help()
  sys.exit()
for option, value in opts:
  if option in ("-h", "--help"):
    help()
    sys.exit()
  elif option in ("-u", "--username"):
    username = value
  elif option in ("-p", "--password"):
    password = value
  elif option in ("-H", "--hostname"):
    hostname = value
  else:
    assert False, "unhandled option"

# Run table update scripts
plugins.Substances.update(username, password, database, hostname);
plugins.Assay2Gene.update(username, password, database, hostname);
##plugins.Assays.update(username, password, database, hostname);
plugins.Activities.update(username, password, database, hostname);
