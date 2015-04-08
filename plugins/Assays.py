import sys
import os
import ConfigParser
from datetime import date
from operator import itemgetter
import zipfile
import gzip
import json
import math
import pypug
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

import logger

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server = "ftp.ncbi.nih.gov"
assayDescriptionFolder = "%s/assays/description" % cfg.get('default', 'tmp')
OVERWRITE_EXISTING_FILES = False

def makedirs(dirs):
  logger.log("creating directories %s" % dirs)
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)


def downloadDescriptions(host, user, passwd, db):
  # Connect to mysql
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, 
    client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  cursor.execute("SELECT DISTINCT(assay_id) FROM Bioassays;")

  # Fetch all aids from the database
  aids = map(itemgetter(0), cursor.fetchall())

  # Begin downloading all descriptions
  for i in range(0, len(aids)):
    logger.log("downloading description for assay (%08d/%08d) %d" %
        (i+1, len(aids), aids[i]))

    # Skip existing files unless OVERWRITE_EXISTING FILES is True
    outFileName = "%s/%s.csv" % (assayDescriptionFolder, aids[i])
    if not os.path.exists(outFileName) or OVERWRITE_EXISTING_FILES:
      with open(outFileName, 'w') as outfile:
        response = pypug.getAssayDescriptionFromAID(aids[i]).encode('utf-8')
        if len(response) > 2:
          description = json.dumps(json.loads(response), separators=(',', ':'))
          outfile.write("%s %s" % (aids[i], description))

def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db,
    client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()

  # Load each description file
  root,_,files = next(os.walk(assayDescriptionFolder))
  for i in range(0, len(files)):
    logger.log("loading files into table (%08d/%08d) %d" %
      (i+1, len(files), aids[i]))
    try:
      query = ("LOAD DATA LOCAL INFILE '%s' REPLACE"
               "INTO TABLE Assay_id_assay_description"
               "FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' ("
               "  assay_id,"
               "  assay_description);")
      cursor.execute(query, os.path.join(root, files[i]))
    except mysql.connector.Error as e:
      logger.error("failed loading data: %s" % str(e))

def update(user, passwd, db, host):
  logger.log("beginning update")
  try:
    directories = [
      assayDescriptionFolder]
    makedirs(directories)
    downloadDescriptions(host, user, passwd, db)
    loadMysqlTable(host, user, passwd, db)
    logger.log("update complete")
  except Exception as e: # Any uncaught errors
    sys.stderr.write(str(e))
    logger.error(str(e))
