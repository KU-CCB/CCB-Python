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
import plugins.Substances
import plugins.Assay2Gene
import plugins.Activities
import plugins.Assays

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

def help():
  usage = ""
  print "---------------------------------------------------------"
  print "%s - The %s database update script" % (__file__, cfg.get('default', 'database'))
  print "---------------------------------------------------------"
  print "For information, see %s." % (cfg.get('repo', 'url'))
  print "Report issues at %s" % cfg.get('repo', 'issues')
  print "contact the author %s <%s>" % (cfg.get('author', 'name'), cfg.get('author', 'email'))
  print ""
  print "Usage: %s [-h|--hostname=127.0.0.1] [-u|--username] [-p|--password]" % __file__
  print ""

# Read command line options
shortargs = "hH:u:p:"
longargs  = ["help","hostname=","username=","password="]
opts, args = getopt.getopt(sys.argv[1:], shortargs, longargs)
hostname, username, password, database = None,None,None,cfg.get('default','database')
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

if hostname is None: hostname = "127.0.0.1"


# Run table update scripts
plugins.Substances.update(username, password, database, hostname);
plugins.Assay2Gene.update(username, password, database, hostname);
plugins.Assays.update(username, password, database, hostname);
plugins.Activities.update(username, password, database, hostname);

