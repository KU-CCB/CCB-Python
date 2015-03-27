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
server      = "ftp.ncbi.nih.gov"
pubchemDir  = "pubchem/Bioassay/Extras"
pubchemFile = "Aid2GiGeneidAccessionUniprot.gz"
localArchive = "%s/%s" % (cfg.get('default','tmp'), pubchemFile)
localFile   = localArchive[:-3]

def downloadFiles():
  logger.log("downloading %s" % pubchemFile)
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  ftp.retrbinary("RETR %s" % pubchemFile, open(localArchive, 'wb').write)
  ftp.quit()

def extractFiles():
  logger.log("extracting %s" % localArchive)
  with gzip.open(localArchive, 'rb') as inf:
    with open(localFile, 'w') as outf:
      for line in inf:
        outf.write(line)

def loadMysqlTable(host, user, passwd, db):
  logger.log("loading %s into MySQL" % localFile)
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    query = (
      "LOAD DATA LOCAL INFILE '%s'"
      " REPLACE"
      " INTO TABLE Aid2GiGeneidAccessionUniprot"
      " FIELDS TERMINATED BY '\t'"
      " LINES TERMINATED BY '\n'"
      " IGNORE 1 LINES ("
      " assay_id,"
      " gi,"
      " gene_id,"
      " ncbi_accession,"
      " uniprot_kb);" % (localFile))
    cursor.execute(query)
    cnx.commit()
  except mysql.connector.Error as e:
    logger.error(str(e))

def update(user, passwd, db, host):
  logger.log("starting update")
  downloadFiles()
  extractFiles()
  loadMysqlTable(host, user, passwd, db)
  logger.log("update complete")
