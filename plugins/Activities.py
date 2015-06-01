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
activityFolder = "%s/activities" % cfg.get('default','tmp')
zippedFolder = "%s/zipped" % activityFolder
unzippedFolder = "%s/unzipped" % activityFolder
ungzippedFolder = "%s/ungzipped" % activityFolder
activityFolderFile = "%s/activities.csv" % ungzippedFolder

def makedirs(dirs):
  logger.log("creating directories: %s" % dirs)
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def downloadFiles():
  server = "ftp.ncbi.nih.gov"
  folder = "pubchem/Bioassay/Concise/CSV/Data"
  mode = "wb"

  logger.log("connection to ftp server at %s" % server)
  ftp = FTP(server)
  ftp.login()
  ftp.cwd(folder)
  files = ftp.nlst()
  logger.log("starting ftp file retrieval at %s" % folder)
  for i in range(0, len(files)):
    logger.log("downloading file (%04d/%04d): %s" % (i+1, len(files), files[i]))
    fileName = "%s/%s" % (zippedFolder, files[i])
    ftp.retrbinary("RETR %s" % files[i], open(fileName, mode).write)
  ftp.quit()


def unzipFiles():
  root, _, files = next(os.walk(zippedFolder))
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
      try:
        filePath = os.path.join(root, folders[i], gzfiles[j])
        f = gzip.open(filePath, 'rb')
        f.readline()
        for line in f:
          line = line.rstrip().split(',')
          # aid, sid (idx 0), cid (idx 1), outcome (idx 2), score (idx 3), url (idx 4).
          activityData.append([aid, line[0], line[1], line[2], line[3], line[4]])
        with open("%s/%s.csv" % (ungzippedFolder, aid), 'w') as outf:
          for line in activityData:                  
            outf.write(",".join(line)+"\n")
      except (OSError, IOError) as e:
        logger.error(e)
      finally:
        f.close()

def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  logger.log("disabling keys and locking table Activities");

  # Disable table keys and lock the Bioassasys table. This will speed up writes since
  # we are making so many LOAD DATA LOCAL INFILE calls. 
  try:
    cursor.execute("ALTER TABLE Activities DISABLE KEYS;");
    cursor.execute("LOCK TABLES Activities, Substances, Assays WRITE;")
    cnx.commit()
  except mysql.connector.Error as e:
    logger.error(str(e))

  logger.log("loading file names from %s" % ungzippedFolder)
  root,_,files = next(os.walk(ungzippedFolder))

  for i in range(0, len(files)):
    logger.log("preloading assay ids from file (%08d/%08d) %s into MySQL table Assays"
      % (i+1, len(files), files[i]))  
    try:
      aid = int(files[i].split('.')[0])
      query = "INSERT IGNORE INTO Assays(assay_id) VALUES(" + str(aid) + ")"
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      logger.error(str(e))
  
    logger.log("preloading substance ids from file (%08d/%08d) %s into MySQL table Substances"
      % (i+1, len(files), files[i]))
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " IGNORE"
        " INTO TABLE Substances"
        " FIELDS TERMINATED BY ','"
        " LINES TERMINATED BY '\n' ("
        "  @assay_id,"
        "  substance_id,"
        "  @compoundId) "
        "SET"
        " compound_id = if(@compoundId in ('', ' ', null), 0, @compoundId);"
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
        "FIELDS TERMINATED BY ',' "
        "LINES TERMINATED BY '\n' ("
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
  directories = [activityFolder, zippedFolder, unzippedFolder, ungzippedFolder];
  try:
    os.makedirs(directories)
    downloadFiles()
    unzipFiles()
    ungzipFiles()
    loadMysqlTable(host, user, passwd, db)
    logger.log("update complete")
  except Exception as e: # Any uncaught errors
    sys.stderr.write(str(e))
    logger.error(str(e))
