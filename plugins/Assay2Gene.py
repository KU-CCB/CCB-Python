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
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

import logger

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server = "ftp.ncbi.nih.gov"
pubchemDir = "pubchem/Bioassay/Extras"
pubchemFile = "Aid2GiGeneidAccessionUniprot.gz"
localArchive = "%s/%s" % (cfg.get('default','tmp'), pubchemFile)
localFile = localArchive[:-3]

def downloadFiles():
  logger.log("downloading %s" % pubchemFile)
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  ftp.retrbinary("RETR %s" % pubchemFile, open(localArchive, 'wb').write)
  ftp.quit()

def extractFiles():
  logger.log("extracting %s" % localArchive)
  try:
    f = gzip.open(localArchive, 'rb')
    with open(localFile, 'w') as outf:
      for line in f: outf.write(line);
  except (OSError, IOError) as e:
    logger.error(str(e))
  finally:
    f.close()

def preloadAssayIds(host, user, passwd, db):
  logger.log("preloading assay_ids from %s into MySQL table Assays" % localFile)
  try:
    cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
    cursor = cnx.cursor()
    query = (
      "LOAD DATA LOCAL INFILE '%s'"
      " IGNORE"
      " INTO TABLE Assays"
      " FIELDS TERMINATED BY '\t'"
      " LINES TERMINATED BY '\n'"
      " IGNORE 1 LINES (assay_id);" % (localFile))
    cursor.execute(query)
    cnx.commit()
  except mysql.connector.Error as e:
    logger.error(str(e))

def loadMysqlTable(host, user, passwd, db):
  logger.log("loading %s into MySQL" % localFile)
  try:
    cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
    cursor = cnx.cursor()
    query = (
      "LOAD DATA LOCAL INFILE '%s'"
      " REPLACE"
      " INTO TABLE Assay2Gene"
      " FIELDS TERMINATED BY '\t'"
      " LINES TERMINATED BY '\n'"
      " IGNORE 1 LINES ("
      " assay_id,"
      " @gi,"
      " @geneId,"
      " ncbi_accession,"
      " uniprot_kb) "
      "SET"
      " gene_id = if(@geneId in ('', ' ', NULL), 0, @geneId),"
      " gi = if(@gi in ('', ' ', NULL), 0, @gi);" % (localFile))
    cursor.execute(query)
    cnx.commit()
  except mysql.connector.Error as e:
    logger.error(str(e))

def update(user, passwd, db, host):
  logger.log("beginning update")
  try:
    downloadFiles()
    extractFiles()
    preloadAssayIds(host, user, passwd, db)
    loadMysqlTable(host, user, passwd, db)
    logger.log("update complete")
  except Exception as e: # any uncaught errors
    sys.stderr.write(str(e))
    logger.error(str(e))
