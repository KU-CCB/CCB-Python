#!/usr/bin/python

import sys
import os
import ConfigParser
from ftplib import FTP
from datetime import date
import zipfile
import gzip
import math as math
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
zippedFolder = "%s/zipped" % activityFolder
unzippedFolder = "%s/unzipped" % activityFolder
ungzippedFolder = "%s/ungzipped" % activityFolder
activityFolderFile = "%s/activities.csv" % ungzippedFolder
lastDownloadRecordFile = "%s/activities/lastDownload.txt" % cfg.get('default', 'tmp')

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
  batchSize = int(math.ceil(len(files)/4.))
  logger.log("checking last download");
  lastDownloadedFile = ""
  start = 0
  if not os.path.isfile(lastDownloadRecordFile):
    open(lastDownloadRecordFile, 'w+')
  else:
    with open(lastDownloadRecordFile, 'r') as ldf:
      lastDownloadedFile = ldf.readline().strip()
      try:
        start = files.index(lastDownloadedFile) + 1
      except ValueError: pass;
  logger.log("begin ftp file retrieval at %s" % server + "/" + pubchemURL)
  end = start + batchSize
  excess = (end % (len(files) - 1)) % end
  end = end - excess
  logger.log("starting download file %s" % files[start])
  for i in range(start, end):
    logger.log("downloading file: (%04d/%04d) %s" % (i+1, batchSize, files[i]))
    ftp.retrbinary("RETR %s" % files[i], open("%s/%s" % (zippedFolder, files[i]), 'wb').write)
  if 
  with open(lastDownloadRecordFile, 'w') as ldf:
    ldf.write(files[end:end+1]);
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
      activityData = []
      with gzip.open(os.path.join(root, folders[i], gzfiles[j]), 'rb') as inf:
        inf.readline()
        for line in inf:
          line = line.rstrip().split(',')
          # * Index:        0    1    2        3      4  
          # * Keep the aid, sid, cid, outcome, score, url 
          # * Discard everything after column 7 (active concentration and data
          #   specified at ftp://ftp.ncbi.nih.gov/pubchem/Bioactivity/Concise/CSV/README)```
          activityData.append([aid, line[0], line[1], line[2], line[3], line[4]])
      with open("%s/%s.csv" % (ungzippedFolder, aid), 'w') as outf:
        for line in activityData:                  
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

  logger.log("loading file names from %s" % ungzippedFolder)
  root,_,files = next(os.walk(ungzippedFolder))
  for i in range(0, len(files)):
    logger.log("preloading assay ids from file (%08d/%08d) %s into MySQL table Assays"
      % (i+1, len(files), files[i]))
    try:
      cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
      cursor = cnx.cursor()
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " IGNORE"
        " INTO TABLE Assays"
        " FIELDS TERMINATED BY '\t'"
        " LINES TERMINATED BY '\n'"
        " IGNORE 1 LINES (assay_id);" % (os.path.join(root, files[i])))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      logger.error(str(e))
    
    logger.log("preloading substance ids from file (%08d/%08d) %s into MySQL table Substances"
      % (i+1, len(files), files[i]))
    try:
      cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
      cursor = cnx.cursor()
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " IGNORE"
        " INTO TABLE Substances"
        " FIELDS TERMINATED BY '\t'"
        " LINES TERMINATED BY '\n'"
        " ("
        "  @assay_id,"
        "  substance_id,"
        "  @compoundId) "
        "SET compound_id = if(@compoundId in ('', ' ', null), 0, @compoundId);"
        % (os.path.join(root, files[i])))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      logger.error(str(e))
   
    logger.log("loading files into table (%08d/%08d) %s" % (i+1, len(files), files[i]))
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s' REPLACE "
        "INTO TABLE Activities "
        "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ("
        " assay_id,"
        " substance_id,"
        " @compoundId,"
        " activity_outcome,"
        " @activityScore,"
        " activity_URL) "
        "SET"
        " activity_score = if(@activityScore = '', null, @activityScore),"
        " compound_id = if(@compoundId = '', 0, @compoundId);"
        % (os.path.join(root, files[i])))
      cursor.execute(query)
      cnx.commit()
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
    ungzippedFolder];
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
