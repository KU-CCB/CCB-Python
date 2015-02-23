#!/usr/bin/python

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
server      = "ftp.ncbi.nih.gov"
pubchemDir  = "pubchem/Bioassay/Extras"
pubchemFile = "Aid2GiGeneidAccessionUniprot.gz"
localArchive = "%s/%s" % (cfg.get('default','tmp'), pubchemFile)
localFile   = localArchive[:-3]

def _downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  ftp.retrbinary("RETR %s" % pubchemFile, open(localArchive, 'wb').write)
  ftp.quit()

def _extractFiles():
  with gzip.open(localArchive, 'rb') as inf:
    with open(localFile, 'w') as outf:
      for line in inf:
        outf.write(line)

def _loadMysqlTable(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    query = (
      "LOAD DATA LOCAL INFILE '%s'"
      " REPLACE"
      " INTO TABLE `Aid2GiGeneidAccessionUniprot`"
      " FIELDS TERMINATED BY '\t'"
      " LINES TERMINATED BY '\n'"
      " IGNORE 1 LINES ("
      " AID,"
      " GI,"
      " GeneID,"
      " Accession,"
      " UniProtKB_ACID);"  % 
      (localFile))
    cursor.execute(query)
    cnx.commit()
  except mysql.connector.Error as e:
    sys.stderr.write("x failed loading data: %e\n" % e)

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> downloading files"
  _downloadFiles()
  print "> extracting files"
  _extractFiles()
  print "> loading %s into table" % localFile
  _loadMysqlTable(user, passwd, db)
  print "> %s complete" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])
