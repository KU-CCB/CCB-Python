#!/usr/bin/python

import sys
import plugins.aid2gigeneid

if len(sys.argv) < 3:
  print "Usage: %s <username> <password>" % __file__
  sys.exit()

username, password = sys.argv[1], sys.argv[2]
server = "ftp.ncbi.nih.gov"
plugins.aid2gigeneid.update(server, username, password)





