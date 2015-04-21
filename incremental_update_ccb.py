#!/usr/bin/python

import sys
"""
mysq.connector is only installed on the acf cluster for python2.6 but we need
to use python2.7+ because the 'with' directive is not backwards compatible
with v2.6.*
"""
import subprocess
import socket
import getopt
if socket.gethostname()[-6:] == "ku.edu":
	sys.path.append('/usr/lib/python2.6/site-packages/')
import ConfigParser
import plugins.incremental_Activities
#import plugins.incremental_Assays
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

def help():
  usage = ""
  print "---------------------------------------------------------"
  print "%s - The incremental %s database update script" % (__file__, cfg.get('default', 'database'))
  print "---------------------------------------------------------"
  print "For information, see %s." % (cfg.get('repo', 'url'))
  print "Report issues at %s" % cfg.get('repo', 'issues')
  print "contact the author %s <%s>" % (cfg.get('author', 'name'), cfg.get('author', 'email'))
  print ""
  print "Usage: %s [-h|--hostname=127.0.0.1] [-u|--username] [-p|--password]" % __file__
  print ""

# Read command line options


hostname = sys.argv[0]
username = sys.argv[1]
password = sys.argv[2]
database = cfg.get('default','database')

if hostname is None: hostname = "127.0.0.1"

# Run table update scripts
plugins.incremental_Activities.update(username, password, database, hostname);
