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
server             = "ftp.ncbi.nih.gov"
assayDescrDir = "%s/assays/description" % cfg.get('default', 'tmp')

def makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def downloadDescriptions(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  cursor.execute("SELECT DISTINCT(assay_id) FROM Bioassays;")
  aids = map(itemgetter(0), cursor.fetchall())
  return;
  for i in range(0, len(aids)):
    sys.stdout.write("\r> downloading description for assay (%08d/%08d)" %
        (i+1, len(aids)))
    sys.stdout.flush()
    with open("%s/%s.csv" % (assayDescrDir, aids[i]), 'w') as outfile:
      # Remove whitespace in response using json.loads and json.dumps
      description = json.dumps(json.loads(
        pypug.getAssayDescriptionFromAID(aids[i]).encode('utf-8')), 
        separators=(',', ': '))
      outfile.write("%s %s" % (aids[i], description))
  sys.stdout.write('\n')

def loadMysqlTable(user, passwd, db):
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  root,_,files = next(os.walk(assayDescrDir))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%08d/%08d)" %
      (i+1, len(files)))
    sys.stdout.flush()
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s'"
        " REPLACE"
        " INTO TABLE Assay_id_assay_description"
        " FIELDS TERMINATED BY ' '"
        " LINES TERMINATED BY '\n' ("
        "   assay_id,"
        "   assay_description);"% os.path.join(root, files[i]))
      cursor.execute(query)
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data: %s\n" % e)
  sys.stdout.write('\n')

def update(user, passwd, db):
  print "plugin: %s" % plugin
  # print "> creating space on local machine"
  # makedirs([assayDescrDir])
  # print "> downloading assay descriptions"
  # downloadDescriptions(user, passwd, db)
  print "> loading data into table"
  loadMysqlTable(user, passwd, db)
  print "> %s complete\n" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])
