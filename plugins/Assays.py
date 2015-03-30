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

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server = "ftp.ncbi.nih.gov"
assayDescriptionFolder = "%s/assays/description" % cfg.get('default', 'tmp')
OVERWRITE_EXISTING_FILES = False

def makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)


def downloadDescriptions(host, user, passwd, db):
  # Connect to mysql
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  cursor.execute("SELECT DISTINCT(assay_id) FROM Bioassays;")

  # Fetch all aids from the database
  aids = map(itemgetter(0), cursor.fetchall())

  # Begin downloading all descriptions
  for i in range(0, len(aids)):
    sys.stdout.write("\r> downloading description for assay (%08d/%08d)" %
        (i+1, len(aids)))
    sys.stdout.flush()
    
    # Skip existing files unless OVERWRITE_EXISTING FILES is True
    outFileName = "%s/%s.csv" % (assayDescriptionFolder, aids[i])
    if not os.path.exists(outFileName) or OVERWRITE_EXISTING_FILES:
      with open(outFileName, 'w') as outfile:
        # Remove whitespace in response before writing the file
        response = pypug.getAssayDescriptionFromAID(aids[i]).encode('utf-8')
        if len(response) > 2:
          description = json.dumps(json.loads(response), separators=(',', ':'))
          outfile.write("%s %s" % (aids[i], description))
  sys.stdout.write('\n')


def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, 
                                client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()

  # Load each description file
  root,_,files = next(os.walk(assayDescriptionFolder))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%08d/%08d)" %
      (i+1, len(files)))
    sys.stdout.flush()
    try:
      query = ("LOAD DATA LOCAL INFILE '%s' REPLACE"
               "INTO TABLE Assay_id_assay_description"
               "FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' ("
               "  assay_id,"
               "  assay_description);"
      cursor.execute(query, os.path.join(root, files[i]))
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data: %s\n" % e)
  sys.stdout.write('\n')

def update(user, passwd, db, host):
  print "plugin: [%s]" % plugin
  print "> creating space on local machine"
  makedirs([assayDescriptionFolder])
  print "> downloading assay descriptions"
  downloadDescriptions(host, user, passwd, db)
  print "> loading data into table"
  loadMysqlTable(host, user, passwd, db)
  print "> %s complete\n" % plugin