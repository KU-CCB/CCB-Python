#!/usr/bin/python
# This is a stub file for a new CCBDB table plugin script

import sys
import os
import ConfigParser
from ftplib import FTP
from datetime import date
import zipfile
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

# only expose the update method from this module
__all__ = ["update"]

# Plugin name
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"

# CCBDB configuration file
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

# NCBI ftp server
server = "ftp.ncbi.nih.gov"

# directory for local data files
localDataDirectory = "%s/plugin/stub/data" % cfg.get('default', 'tmp')

def _makedirs(dirs):
  """
  Create the directories necessary for this plugin to run
  Tip: create all directories as subfolders of the top-level 'data/' directory
  """
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def _loadMysqlTable(user, passwd, db):
  """
  Load the generated data files into the database
  """
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  _,_,files = next(os.walk(localDataDirectory))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%s/%s)" % (i, len(files)))
    sys.stdout.flush()
    try:
      query = (
          "LOAD DATA LOCAL INFILE '%s/%s/%s'"
          " REPLACE"
          " INTO TABLE `pluginStub`"
          " FIELDS TERMINATED BY ','"
          " LINES TERMINATED BY '\n'"
          "();" % 
          (os.getcwd(), localDataDirectory, files[i]))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data: {}\n".format(e))

def update(user, passwd, db):
  """
  Run the update
  """
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localDir, localUnzippedDir, localUngzippedDir])
  # .. obtain data here
  print "> loading data into table"
  _loadMysqlTable(user, passwd, db)
  print "> %s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])