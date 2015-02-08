#!/usr/bin/python

import sys
import os
import subprocess
from ftplib import FTP
import gzip 
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

__all__ = ["update"]

plugin = __name__[__name__.index('.')+1:]
TABLE_NAME = "Compounds"
server = "ftp.ncbi.nih.gov"
pubchemDir  = "pubchem/Compound/Weekly"
foundUpdated = False
localDir = "./data/compounds"
localFiles = ["smiles.csv", "chemattr.csv"]

def _makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def _checkForUpdates(folder):
  global pubchemDir
  global foundUpdated
  folder = folder.split()
  lastUpdated, name = folder[-1], folder[-1]
  sevenDaysAgo = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
  if sevenDaysAgo < lastUpdated:
    pubchemDir = "%s/SDF" % name
    foundUpdated = True

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localDir])
  print "> building client binaries"
  make_process = subprocess.Popen("make")
  if make_process.wait() != 0:
    print "x failed to building client binaries. Check ./plugins/store/makefile for errors."
  print "> checking for updated files"
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  ftp.retrlines("LIST", _checkForUpdates)
  if foundUpdated:
    print "> downloading files %s " % pubchemDir
    ftp.cwd(pubchemDir)
    for pubchemFile in ftp.nlst():
      print pubchemFile
      localFile = "%s/%s" % (localDir, pubchemFile)
      ftp.retrbinary("RETR %s" % pubchemFile, open("%s" % localFile, 'wb').write)
  ftp.quit()
  sys.exit()
  # Fix below
  print "> extracting files"
  with gzip.open("%s/data/%s" % (os.getcwd(), localFile), 'rb') as inf:
    with open("%s/data/%s" % (os.getcwd(), localFile), 'w') as outf:
      for line in inf:
        outf.write(line)
  print "> loading %s into table" % localFile
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    cursor.execute(
        "LOAD DATA LOCAL INFILE '%s/data/%s'"
        " REPLACE"
        " INTO TABLE %s"
        " FIELDS TERMINATED BY '\t'"
        " LINES TERMINATED BY  '\n'"
        " IGNORE 1 LINES ("
        " AID,"
        " GI,"
        " GeneID,"
        " Accession,"
        " UniProtKB_ACID);"  % 
        (os.getcwd(), localFile, TABLE_NAME))
  except mysql.connector.Error as e:
    sys.stderr.write("x Failed loading data: {}\n".format(e))
    return
  print "> %s complete" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])