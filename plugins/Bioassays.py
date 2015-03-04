#!/usr/bin/python

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

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server            = "ftp.ncbi.nih.gov"
pubchemDir        = "pubchem/Bioassay/Concise/CSV/Data"
assayDataDir      = "%s/assays"      % cfg.get('default','tmp')
substanceDataDir  = "%s/substances"  % cfg.get('default','tmp')
localZippedDir    = "%s/zipped"      % assayDataDir
localUnzippedDir  = "%s/unzipped"    % assayDataDir
localUngzippedDir = "%s/ungzipped"   % assayDataDir
localProcessedDir = "%s/processed"   % assayDataDir
assayDataFile     = "%s/assays.csv"  % localProcessedDir
sid2cidMapFile    = "%s/sid2cid.csv" % substanceDataDir

def makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  files = ftp.nlst()
  for i in range(0, len(files)):
    sys.stdout.write("\r> downloading files (%04d/%04d)" % (i+1, len(files)))
    sys.stdout.flush()
    ftp.retrbinary("RETR %s" % files[i], open("%s/%s" % (localZippedDir, files[i]), 'wb').write)
  sys.stdout.write('\n')
  ftp.quit()

def unzipFiles():
  root,_,files = next(os.walk(localZippedDir))
  for i in range(0, len(files)):
    sys.stdout.write("\r> unzipping files (%04d/%04d)" % (i+1, len(files)))
    sys.stdout.flush()
    archive = zipfile.ZipFile(os.path.join(root, files[i]), 'r')
    archive.extractall(localUnzippedDir)
  sys.stdout.write("\n")

def splitDataFiles():
  fileNo = 0
  root,folders,_ = next(os.walk(localUnzippedDir))
  # Iterate over each folder containing gzipped files
  for j in range(0, len(folders)):
    _,_,gzfiles = next(os.walk(os.path.join(root, folders[j])))
    # Iterate over each gzipped file in the folder
    for i in range(0, len(gzfiles)):
      sys.stdout.write("\r> splitting folder (%04d/%04d) files (%04d/%04d)" %
        (j+1, len(folders), i+1, len(gzfiles)))
      aid = gzfiles[i][:gzfiles[i].index('.')]
      sid2cidData, assayData = [], []
      with gzip.open(os.path.join(root, folders[j], gzfiles[i]), 'rb') as inf:
        inf.readline()
        for line in inf:
          line = line.rstrip().split(',')
          # - Index:        0    2        3      4    5
          # - Keep the aid, sid, outcome, score, url, comment
          # - Discard everything after column 7 (active concentration and data
          #   specified at ftp://ftp.ncbi.nih.gov/pubchem/Bioassay/Concise/CSV/README)```
          assayData.append([aid, line[0], line[2], line[3], line[4], line[5]])
          # Now store the sid/cid map data
          #            0       1
          # - Keep the sid and cid
          sid2cidData.append([aid, line[0], line[1]])
      with open("%s/%s.csv" % (localProcessedDir, aid), 'w') as outf:
        for line in assayData:                  
          outf.write(",".join(line)+"\n")
      with open(sid2cidMapFile, 'a') as outf: 
        for line in sid2cidData:
          outf.write(",".join(line)+"\n")
     
def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  # Prepare table for insertion
  print "> preparing mysql"
  try:
    cursor.execute("ALTER TABLE `Bioassays` DISABLE KEYS;");
    cursor.execute("LOCK TABLES `Bioassays` WRITE;")
  except mysql.connector.Error as e:
    sys.stderr.write("x failed preparing Bioassays: %s\n" % e)
    
  # Execute insertions
  print "> gathering filed to be inserted..."
  root,_,files = next(os.walk(localProcessedDir))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%08d/%08d)" % (i+1, len(files)))
    try:
      query = (
          "LOAD DATA LOCAL INFILE '%s'"
          " REPLACE"
          " INTO TABLE Bioassays"
          " FIELDS TERMINATED BY ','"
          " LINES TERMINATED BY '\n' ("
          "   assay_id,"
          "   substance_id,"
          "   activity_outcome,"
          "   activity_score,"
          "   activity_URL,"
          "   assay_comment"
          ");" % (os.path.join(root, files[i])))
      cursor.execute(query)
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data into Bioassays: %s\n" % e)

  # Commit changes, unlock the table and rebuild indexes
  print "> rebuilding table indexes" 
  try:
    cursor.execute("UNLOCK TABLES;")
    cursor.execute("ALTER TABLE `Bioassays` ENABLE KEYS;")
    cnx.commit()
  except mysql.connector.Error as e:
    sys.stderr.write("x failed re-enabling keys on Bioassays: %s\n" % e)
  cursor.close()
  cnx.close()

def update(user, passwd, db, host="127.0.0.1"):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  makedirs([assayDataDir, localZippedDir, localUnzippedDir, 
             localUngzippedDir, localProcessedDir, substanceDataDir])
  print "> downloading updated files"
  downloadFiles()
  print "> unzipping files"
  unzipFiles()
  print "> begin splitting data into separate files"
  splitDataFiles()
  print "> loading data into table"
  loadMysqlTable(host, user, passwd, db)
  print "> %s complete\n" % plugin
