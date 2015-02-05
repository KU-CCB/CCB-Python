#!/usr/bin/python

import sys
import ConfigParser
import plugins.aid2gigeneid

if len(sys.argv) < 3:
  print "Usage: %s <username> <password>" % __file__
  sys.exit()

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
user, passwd = sys.argv[1], sys.argv[2]
plugins.aid2gigeneid.update(user, passwd, cfg.get('default','db'))