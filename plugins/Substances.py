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
from subprocess import call
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag
import logger

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
file = "CID-SID.gz"
substanceFolder = "%s/substances" % cfg.get('default', 'tmp')
processedFolder = "%s/processed" % substanceFolder
archive = "%s/%s" % (substanceFolder, file)
extractedFile = archive[:-3]

def mkdirs():
  for directory in ["data/substances/processed/"]:
    if not os.path.exists(directory):
      os.makedirs(directory)

def downloadFiles():
  server = "ftp.ncbi.nih.gov"
  folder = "pubchem/Compound/Extras"
  logger.log("downloading %s from %s" % (file, server))
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(folder)
  ftp.retrbinary("RETR %s" % file, open(archive, 'wb').write)
  ftp.quit()

def extractFiles():
  logger.log("extracting %s to %s" % (archive, extractedFile))
  try:
    f = gzip.open(archive, 'rb') 
    with open(extractedFile, 'w') as outf:
      for line in f: outf.write(line);
  except (OSError, IOError) as e:
    logger.error(str(e))
  finally:
    f.close()	

def splitFiles():
  chunkSize = "1000000"
  suffixLen = "3"
  prefix = "%s/" % processedFolder
  logger.log("splitting %s into %s-line files" % (extractedFile, chunkSize))
  call(["split", extractedFile, "-l", chunkSize, "-d", "-a", suffixLen, prefix])

def loadMysqlTable(host, user, passwd, db):
  logger.log("connecting to mysql")
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  _,_,files = next(os.walk(processedFolder))
  for i in range(0, len(files)):
    logger.log("loading file (%04d/%04d) %s into mysql" % 
      (i+1, len(files), files[i]))
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " REPLACE"
        " INTO TABLE Substances"
        " FIELDS TERMINATED BY '\t'"
        " LINES TERMINATED BY '\n'"
        " ("
        " compound_id,"
        " substance_id,"
        " @type);" % (processedFolder + "/" + files[i]))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      logger.error("x failed loading data: %s" % str(e))

def update(user, passwd, db, host):
  logger.log("beginning update")
  mkdirs()
  downloadFiles()
  extractFiles()
  splitFiles()
  loadMysqlTable(host, user, passwd, db)
  logger.log("update complete")
