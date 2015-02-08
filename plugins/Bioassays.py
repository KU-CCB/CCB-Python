#!/usr/bin/python
# Implementation needs to be tested on db2 servers. Postponing development of 
# this file for now.

import sys
import os
from ftplib import FTP
from datetime import date
import zipfile
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

__all__ = ["update"]

plugin = __name__[__name__.index('.')+1:]
TABLE_NAME = "Bioassays"
server = "ftp.ncbi.nih.gov"
pubchemDir = "pubchem/Bioassay/Concise/CSV/Data"
localDir = "./data/assays"
localUnzippedDir = "./data/assays/unzipped"
localRawDir = "./data/assays/ungzipped"
localDataFile = "./data/assays/fullData.csv"
header = ("PUBCHEM_AID,PUBCHEM_SID,PUBCHEM_CID,PUBCHEM_ACTIVITY_OUTCOME,PUBCHEM_ACTIVITY_SCORE,PUBCHEM_ACTIVITY_URL,PUBCHEM_ASSAYDATA_COMMENT")

def _makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def _downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  files = ftp.nlst()
  for pubchemFile in files:
      ftp.retrbinary("RETR %s" % pubchemFile, open("%s/%s" % (localDir, pubchemFile), 'wb').write)
  ftp.quit()

def _unzipFiles():
  directory, _, files = next(os.walk(localDir))
  for zippedFile in files:
    archive = zipfile.ZipFile("%s/%s" % (directory, zippedFile), 'r')
    archive.extractall(localUnzippedDir)

def _concatFiles():
  with open(localDataFile, 'w') as outf:
    outf.write(header)
  directory,folders,_ = next(os.walk(localUnzippedDir))
  for folder in folders:
    _,_,files = next(os.walk("%s/%s" % (directory, folder)))
    for gzfile in files:
      aid = gzfile[:gzfile.index('.')]
      with gzip.open("%s/%s/%s" % (directory, folder, gzfile), 'rb') as inf:
        with open(localDataFile, 'a') as outf:
          inf.readline() # discard header
          for line in inf:
            outf.write("%s,%s" % (aid, line))

def _loadMysqlTable(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    cursor.execute(
        "LOAD DATA LOCAL INFILE '%s'"
        " REPLACE"
        " INTO TABLE %s"
        " FIELDS TERMINATED BY ','"
        " LINES TERMINATED BY  '\n'"
        " IGNORE 1 LINES ("
        "   AID,"
        "   SID,"
        "   CID,"
        "   Activity_Outcome,"
        "   Activity_Score,"
        "   Activity_URL,"
        "   Comment"
        ");" % 
        (localDataFile, TABLE_NAME))
  except mysql.connector.Error as e:
    sys.stderr.write("Failed loading data: {}\n".format(e))

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localDir, localUnzippedDir, localRawDir])
  print "> downloading updated files"
  _downloadFiles()
  print "> unzipping files"
  _unzipFiles()
  print "> compiling gz files into single data file"
  _concatFiles()
  print "> loading data into table"
  _loadMysqlTable(user, passwd, db)
  print "> %s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])