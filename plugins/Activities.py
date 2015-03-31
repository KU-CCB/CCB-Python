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

import logger

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server = "ftp.ncbi.nih.gov"
pubchemURL = "pubchem/Bioassay/Concise/CSV/Data"
activityFolder = "%s/activities" % cfg.get('default','tmp')
substanceFolder = "%s/substances" % cfg.get('default','tmp')
zippedFolder = "%s/zipped" % activityFolder
unzippedFolder = "%s/unzipped" % activityFolder
ungzippedFolder = "%s/ungzipped" % activityFolder
activityFolderFile = "%s/activities.csv" % ungzippedFolder
sid2cidMapFile = "%s/sid2cid.csv" % substanceFolder

def makedirs(dirs):
  logger.log("creating directories: %s" % dirs)
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemURL)
  files = ftp.nlst()
  logger.log("begin ftp file retrieval at %s" % server + "/" + pubchemURL)
  for i in range(0, len(files)):
    logger.log("downloading file: (%04d/%04d) %s" % (i+1, len(files), files[i]))
    ftp.retrbinary("RETR %s" % files[i], open("%s/%s" % (zippedFolder, files[i]), 'wb').write)
  ftp.quit()

def unzipFiles():
  root,_,files = next(os.walk(zippedFolder))
  for i in range(0, len(files)):
    try:
      logger.log("unzipping file: (%04d/%04d) %s" % (i+1, len(files), files[i]))
      archive = zipfile.ZipFile(os.path.join(root, files[i]), 'r')
      archive.extractall(unzippedFolder)
    except zipfile.BadZipfile as e:
      logger.error(str(e))

def ungzipFiles():
  root,folders,_ = next(os.walk(unzippedFolder))
  for i in range(0, len(folders)):
    _,_,gzfiles = next(os.walk(os.path.join(root, folders[i])))
    for j in range(0, len(gzfiles)):
      logger.log("ungzipping folder (%04d/%04d) file (%04d/%04d) %s" % 
        (i+1, len(folders), j+1, len(gzfiles), gzfiles[j]))
      aid = gzfiles[j][:gzfiles[j].index('.')]
      sid2cidData, activityData = [], []
      with gzip.open(os.path.join(root, folders[i], gzfiles[j]), 'rb') as inf:
        inf.readline()
        for line in inf:
          line = line.rstrip().split(',')
          # * Index:        0    1    2        3      4  
          # * Keep the aid, sid, cid, outcome, score, url 
          # * Discard everything after column 7 (active concentration and data
          #   specified at ftp://ftp.ncbi.nih.gov/pubchem/Bioactivity/Concise/CSV/README)```
          activityData.append([aid, line[0], line[1], line[2], line[3], line[4]])
          # * Index:   0       1
          # * Keep the sid and cid
          # * We have to store the sid and cid data from these files in a separate database
          #   table, so we'll write the file here and pass it off to the next table:
          #   Substance_id_compound_id
          sid2cidData.append([aid, line[0], line[1]])
      with open("%s/%s.csv" % (ungzippedFolder, aid), 'w') as outf:
        for line in activityData:                  
          outf.write(",".join(line)+"\n")
      with open(sid2cidMapFile, 'a') as outf: 
        for line in sid2cidData:
          outf.write(",".join(line)+"\n")
     

def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  # Disable table keys and lock the Bioassasys table. This will speed up writes since
  # we are making so many LOAD DATA LOCAL INFILE calls.
  logger.log("disabling keys and locking table Activities");
  try:
    cursor.execute("ALTER TABLE `Activities` DISABLE KEYS;");
    cursor.execute("LOCK TABLES `Activities` WRITE;")
  except mysql.connector.Error as e:
    logger.error(str(e))
    
  root,_,files = next(os.walk(ungzippedFolder))
  for i in range(0, len(files)):
    logger.log("loading files into table (%08d/%08d) %s" % (i+1, len(files), files[i]))
    try:
      query = (
          "LOAD DATA LOCAL INFILE '%s' REPLACE "
          "INTO TABLE Activities "
          "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ("
          " assay_id,"
          " substance_id,"
          " compound_id,"
          " activity_outcome,"
          " activity_score,"
          " activity_URL);" % (os.path.join(root, files[i])))
      cursor.execute(query)
    except mysql.connector.Error as e:
      logger.error(str(e))

  # Unlock the tables and rebuild  indexes. This can also take a very long time
  # to complete.
  try:
    cursor.execute("UNLOCK TABLES;")
    cursor.execute("ALTER TABLE `Activities` ENABLE KEYS;")
  except mysql.connector.Error as e:
    logger.error(str(e))
  cursor.close()
  cnx.close()


def update(user, passwd, db, host):
  logger.log("beginning update")
  directories = [
    activityFolder, 
    zippedFolder, 
    unzippedFolder, 
    ungzippedFolder, 
    substanceFolder];
  try:
    makedirs(directories)
    downloadFiles()
    unzipFiles()
    ungzipFiles()
    loadMysqlTable(host, user, passwd, db)
    logger.log("update complete")
  except Exception as e: # Any uncaught errors
    sys.stderr.write(str(e))
    logger.error(str(e)) 
