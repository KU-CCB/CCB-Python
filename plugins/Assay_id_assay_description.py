import sys
import os
import ConfigParser
from datetime import date
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
server             = "ftp.ncbi.nih.gov"
assayDescrDir = "%s/assays/description"

def makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def downloadDescriptions():
  for aid in aids:
    with open("%s/%s.csv" % (assayDescrDir, aid), 'w') as outfile:
      # Remove whitespace in response using json.loads and json.dumps
      description = json.dumps(json.loads(
        pypug.getAssayDescriptionFromAID(aid).encode('utf-8')), 
        separators=(',', ': '))
      outfile.write("%s %s" % (aid, description))

def loadMysqlTable(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  cursor.execute("SELECT DISTINCT(assay_id) FROM Bioassays;")
  sys.exit()
  root,_,files = next(os.walk(assayDescrDir))
  for i in range(0, len(files)):
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " REPLACE"
        " INTO TABLE Assay_id_assay_description"
        " FIELDS TERMINATED BY ' '"
        " LINES TERMINATED BY '\n' ("
        "   assay_id,"
        "   assay_description);")
      cursor.execute(query, os.path.join(root, files[i]))
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data: %s\n" % e)

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  makedirs([])
  # print "> downloading assay descriptions"
  # downloadDescriptions()
  print "> loading data into table"
  loadMysqlTable(user, passwd, db)
  print "> %s complete\n" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])
