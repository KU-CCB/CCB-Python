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

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server = "ftp.ncbi.nih.gov"
pubchemURL = "pubchem/Bioactivity/Concise/CSV/Data"
activityFolder = "%s/activities" % cfg.get('default','tmp')
substanceFolder = "%s/substances" % cfg.get('default','tmp')
zippedFolder = "%s/zipped" % activityFolder
unzippedFolder = "%s/unzipped" % activityFolder
ungzippedFolder = "%s/ungzipped" % activityFolder
activityFolderFile = "%s/activities.csv" % ungzippedFolder
sid2cidMapFile = "%s/sid2cid.csv" % substanceFolder


def makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)


def downloadFiles():
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemURL)
  files = ftp.nlst()
  for i in range(0, len(files)):
    sys.stdout.write("\r> downloading files (%04d/%04d)" % (i+1, len(files)))
    sys.stdout.flush()
    ftp.retrbinary("RETR %s" % files[i], open("%s/%s" % (zippedFolder, files[i]), 'wb').write)
  sys.stdout.write('\n')
  ftp.quit()


def unzipFiles():
  root,_,files = next(os.walk(zippedFolder))
  for i in range(0, len(files)):
    sys.stdout.write("\r> unzipping files (%04d/%04d)" % (i+1, len(files)))
    sys.stdout.flush()
    archive = zipfile.ZipFile(os.path.join(root, files[i]), 'r')
    archive.extractall(unzippedFolder)
  sys.stdout.write("\n")


def ungzipFiles():
  root,folders,_ = next(os.walk(unzippedFolder))
  for i in range(0, len(folders)):
    _,_,gzfiles = next(os.walk(os.path.join(root, folders[i])))
    for j in range(0, len(gzfiles)):
      sys.stdout.write("\r> ungzipping folder (%04d/%04d) file (%04d/%04d)" % 
        (i+1, len(folders), j+1, len(gzfiles)))
      sys.stdout.flush()

      aid = gzfiles[j][:gzfiles[j].index('.')]
      sid2cidData, activityData = [], []
      with gzip.open(os.path.join(root, folders[i], gzfiles[j]), 'rb') as inf:
        inf.readline()
        for line in inf:
          line = line.rstrip().split(',')
          # * Index:        0    2        3      4    5
          # * Keep the aid, sid, outcome, score, url, comment
          # * Discard everything after column 7 (active concentration and data
          #   specified at ftp://ftp.ncbi.nih.gov/pubchem/Bioactivity/Concise/CSV/README)```
          activityData.append([aid, line[0], line[2], line[3], line[4], line[5]])
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
    sys.stdout.write('\n')
     

def loadMysqlTable(host, user, passwd, db):
  cnx = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()

  # Disable table keys and lock the Bioassasys table. This will speed up writes since
  # we are making so many LOAD DATA LOCAL INFILE calls.
  try:
    cursor.execute("ALTER TABLE `Activities` DISABLE KEYS;");
    cursor.execute("LOCK TABLES `Activities` WRITE;")
  except mysql.connector.Error as e:
    sys.stderr.write("x failed preparing Activities: %s\n" % e)
    
  root,_,files = next(os.walk(ungzippedFolder))
  for i in range(0, len(files)):
    sys.stdout.write("\r> loading files into table (%08d/%08d)" % (i+1, len(files)))
    sys.stdout.flush()
    try:
      query = (
          "LOAD DATA LOCAL INFILE '%s' REPLACE "
          "INTO TABLE Activities "
          "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ("
          " activity_id,"
          " substance_id,"
          " activity_outcome,"
          " activity_score,"
          " activity_URL,"
          " activity_comment);" % (os.path.join(root, files[i])))
      cursor.execute(query)
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data into Activities: %s\n" % e)
  sys.stdout.write('\n')

  # Unlock the tables and rebuild  indexes. This can also take a very long time
  # to complete.
  try:
    cursor.execute("UNLOCK TABLES;")
    cursor.execute("ALTER TABLE `Activities` ENABLE KEYS;")
  except mysql.connector.Error as e:
    sys.stderr.write("x failed re-enabling keys on Activities: %s\n" % e)

  cursor.close()
  cnx.close()


def update(user, passwd, db, host):
  print "plugin: [%s]" % plugin
  print "> creating space on local machine"
  makedirs([activityFolder, zippedFolder, unzippedFolder, 
             ungzippedFolder, substanceFolder])
  print "> downloading updated files"
  downloadFiles()
  print "> unzipping files"
  unzipFiles()
  print "> begin splitting data into separate files"
  ungzipFiles()
  print "> loading data into table"
  loadMysqlTable(host, user, passwd, db)
  print "> %s complete\n" % plugin
