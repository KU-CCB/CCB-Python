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
pubchemDir = "pubchem/Compound/Weekly"
foundUpdated = False
localDir = "./data/compounds"
localUngzippedDir = "./data/compounds/ungzipped"
localParsedDir = "./data/compounds/parsed"
localFiles = ["smiles.csv", "chemattr.csv"]
fullDataFile = "./data/compounds/parsed/fullData.csv"

def _makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def _getUpdatedFolder(ftp):
  ftp.cwd(pubchemDir)
  for folderDate in ftp.nlst():
    sevenDaysAgo = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    if sevenDaysAgo < folderDate:
      return "%s/SDF" % folderDate
  return None

def _downloadFolder(ftp, folder):
  ftp.cwd(folder)
  for pubchemFile in ftp.nlst():
    localFile = "%s/%s" % (localDir, pubchemFile)
    ftp.retrbinary("RETR %s" % pubchemFile, open("%s" % localFile, 'wb').write)

def _extractFiles():
  path,_,files = next(os.walk(localDir))
  for f in files:
    with gzip.open("%s/%s" % (localDir, f), 'rb') as inf:
      with open("%s/%s" % (localUngzippedDir, f[:-3]), 'w') as outf:
        for line in inf:
          outf.write(line)

def _parseAndConcatenateFiles():
  path,_,files = next(os.walk(localUngzippedDir))
  for f in files:
    subprocess.Popen(["./plugins/store/xattr", path+'/'+f , "./data/compounds/parsed/%s" % f[:-4]])
  subprocess.Popen(["cat", "./data/compounds/parsed/* > %s" % fullDataFile])

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localDir, localUngzippedDir, localParsedDir])
  print "> checking for updated files"
  ftp = FTP(server)
  ftp.login() # anonymous
  folder = _getUpdatedFolder(ftp)
  if not folder is None:
    print "> downloading files %s " % folder
    _downloadFolder(ftp, folder)
  ftp.quit()
  print "> extracting files"
  _extractFiles()
  print "> processing files"
  _parseAndConcatenateFiles()
  # print "> loading %s into table" % localFile
  # cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  # cursor = cnx.cursor()
  # try:
  #   cursor.execute(
  #       "LOAD DATA LOCAL INFILE '%s/data/%s'"
  #       " REPLACE"
  #       " INTO TABLE %s"
  #       " FIELDS TERMINATED BY '\t'"
  #       " LINES TERMINATED BY  '\n'"
  #       " IGNORE 1 LINES ("
  #       " AID,"
  #       " GI,"
  #       " GeneID,"
  #       " Accession,"
  #       " UniProtKB_ACID);"  % 
  #       (os.getcwd(), localFile, TABLE_NAME))
  # except mysql.connector.Error as e:
  #   sys.stderr.write("x Failed loading data: {}\n".format(e))
  #   return
  print "> %s complete" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])