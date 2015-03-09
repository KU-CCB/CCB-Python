#!/usr/bin/python

"""
This file downloads and stores the file Aid2GiGeneidAccessionUniprot from
PubChem found at ftp.ncbi.nih.gov/pubchem/Bioassay/Extras
"""

import sys
import os
import ConfigParser
from ftplib import FTP
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server          = "ftp.ncbi.nih.gov"
pubchemFolder   = "pubchem/Compound/Extras"
pubchemFile     = "CID-SID.gz"
substanceFolder = "%s/substances" % cfg.get('default', 'tmp')
localArchive    = "%s/%s" % (substanceFolder, pubchemFile)
localFile       = localArchive[:-3]

def downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemFolder)
  ftp.retrbinary("RETR %s" % pubchemFile, open(localArchive, 'wb').write)
  ftp.quit()

def extractFiles():
  with gzip.open(localArchive, 'rb') as inf:
    with open(localFile, 'w') as outf:
      for line in inf:
        outf.write(line)

def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    query = (
      "LOAD DATA LOCAL INFILE '%s'"
      " REPLACE"
      " INTO TABLE Substance_id_compound_id"
      " FIELDS TERMINATED BY '\t'"
      " LINES TERMINATED BY '\n'"
      " IGNORE 1 LINES ("
      " compound_id,"
      " substance_id,"
      " @type);" % localFile)
    cursor.execute(query)
    cnx.commit()
  except mysql.connector.Error as e:
    sys.stderr.write("x failed loading data: %s\n" % e)

def update(user, passwd, db, host):
  print "plugin: [%s]" % plugin
  print "> downloading files"
  downloadFiles()
  print "> extracting files"
  extractFiles()
  print "> loading %s into table" % localFile
  loadMysqlTable(host, user, passwd, db)
  print "> %s complete" % plugin
