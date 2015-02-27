#!/usr/bin/python
# Implementation needs to be tested on db2 servers. Postponing development of 
# this file for now.

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
localDir          = "%s/assays" % cfg.get('default','tmp')
localUnzippedDir  = "%s/assays/unzipped" % cfg.get('default','tmp')
localUngzippedDir = "%s/assays/ungzipped" % cfg.get('default','tmp')
localProcessedDir = "%s/assays/processed" % cfg.get('default', 'tmp')
sid2cidMapFile    = "%s/sid2cid.csv" % localProcessedDir
aid2urlMapFile    = "%s/aid2url.csv" % localProcessedDir

# For human readability
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
  for i in range(0, len(files)):
    sys.stdout.write("\r> downloading files (%s/%s)" % (i, len(files)))
    sys.stdout.flush()
    ftp.retrbinary("RETR %s" % files[i], open("%s/%s" % (localDir, files[i]), 'wb').write)
  sys.stdout.write('\n')
  ftp.quit()

def _unzipFiles():
  directory,_,files = next(os.walk(localDir))
  for zippedFile in files:
    archive = zipfile.ZipFile("%s/%s" % (directory, zippedFile), 'r')
    archive.extractall(localUnzippedDir)

def _splitDataFiles():
  directory,folders,_ = next(os.walk(localUnzippedDir))
  assayData = []
  sid2cidData = []
  # This algorithm uses a lot of memory. It loads all data for all files into
  # memory and writes to disk one time. This should be fast since the ITTC
  # cluster has enough RAM to hold all of this data. However if you're running
  # this on your own machine, you might want to have 8+ GB of memory.
  for j in range(0, len(folders)):
    _,_,gzfiles = next(os.walk("%s/%s" % (directory, folders[j])))
    for i in range(0, len(gzfiles)):
      sys.stdout.write("\r> processing folder (%s/%s) files (%s/%s)" % 
        (j, len(folders), i, len(gzfiles)))
      sys.stdout.flush()
      aid = gzfiles[i][:gzfiles[i].index('.')]
      with gzip.open("%s/%s/%s" % (directory, folders[j], gzfiles[i]), 'rb') as inf:
        # We could iterate over each line in the file and use less memory, but
        # this would also require opening and closing each output file in this 
        # loop which would be rather slow. Each file is only a couple hundred MB
        # so we can afford to simply load all of the data for a file one time
        # and open and close each output file one time per input file.
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
  with open(localProcessedDir+"/alldata.csv", 'a') as outf:
    for line in assayData:                  
      outf.write(",".join(line)+"\n")
  with open(sid2cidMapFile, 'a') as outf: 
    for line in sid2cidData:
      outf.write(",".join(line)+"\n")

def _loadMysqlTable(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  # Prepare table for insertion
  try:
    query = "FLUSH TABLES;"
    cursor.execute(query)
    query = "ALTER TABLE `Bioassays` DISABLE KEYS;";
    cursor.execute(query);
  except mysql.connector.Error as e:
    sys.stderr.write("x failed preparing Bioassays: %e\n" % e)
  # Execute insertions
  path,_,files = next(os.walk(localUngzippedDir))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%s/%s)" % (i, len(files)))
    sys.stdout.flush()
    try:
      query = (
          "LOAD DATA LOCAL INFILE '%s/%s'"
          " REPLACE"
          " INTO TABLE `Bioassays`"
          " FIELDS TERMINATED BY ','"
          " LINES TERMINATED BY '\n'"
          " IGNORE 1 LINES ("
          "   asssay_id,"
          "   substance_id,"
          "   activity_outcome,"
          "   activity_score,"
          "   activity_URL,"
          "   assay_comment"
          ");" % 
          (path, files[i]))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data into Bioassays: %e\n" % e)
  # Rebuild indexes
  try:
    query = "ALTER TABLE `Bioassays` ENABLE KEYS;";
    cursor.execute(query)
  except mysql.connector.Error as e:
    sys.stderr.write("x failed re-enabling keys on Bioassays: %e\n" % e)

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localDir, localUnzippedDir, localUngzippedDir, localProcessedDir])
  # print "> downloading updated files"
  # _downloadFiles()
  # print "> unzipping files"
  # _unzipFiles()
  print "> begin splitting data into separate files"
  _splitDataFiles()
  # print "> loading data into table"
  # _loadMysqlTable(user, passwd, db)
  print "> %s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])