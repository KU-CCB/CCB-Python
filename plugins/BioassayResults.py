#!/usr/bin/python
# Implementation needs to be tested on db2 servers. Postponing development of 
# this file for now.

import sys
import os
from ftplib import FTP
from datetime import date
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

TABLE_NAME = "BioassayResults"
pcdir = "pubchem/Bioassay/Concise/CSV/Data"
server = "ftp.ncbi.nih.gov"
cacheFile = "./plugins/tmp/brcache.txt"
lines = []
infocache = {}

def _downloadIfUpdated(ftp, filedata):
  data = filedata.split()
  fname, fsize, = data[8], data[4]
  ftp.retrbinary("RETR %s" % fname, open("./data/%s" % fname, 'wb').write)
  return
  if fname in infocache:
    if infocache[fname] != fsize:
      ftp.retrbinary("RETR %s" % fname, open("./data/%s" % fname, 'wb').write)
  else:
    infocache[fname] = fsize
    ftp.retrbinary("RETR %s" % fname, open("./data/%s" % fname, 'wb').write)

def update(user, passwd, db):
  print "plugin: %s" % __name__
  print "--downloading updated files"
  if not os.path.exists(cacheFile):
    open(cacheFile, 'w').close()
  with open(cacheFile, 'r') as inf:
    for line in inf:
      (key, value) = line.split()
      infocache[key] = value
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pcdir)
  ftp.retrlines('LIST', lambda line: lines.append(line))
  for filedata in lines:
    _downloadIfUpdated(ftp, filedata)
  ftp.quit()
  with open(cacheFile, 'w') as outf:
    for key,value in infocache.iteritems():
      outf.write("%s %s\n" % (key, value))
  sys.exit()
#   print "--unzipping files"
#   with gzip.open("%s/data/%s" % (os.getcwd(), remfile), 'rb') as inf:
#     with open("%s/data/%s" % (os.getcwd(), locfile), 'w') as outf:
#       for line in inf:
#         outf.write(line)
#   print "--loading %s into table" % remfile
#   cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
#   cursor = cnx.cursor()
#   try:
#     cursor.execute(
#         "LOAD DATA LOCAL INFILE '%s/data/%s'"
#         " REPLACE"
#         " INTO TABLE %s"
#         " FIELDS TERMINATED BY '\t'"
#         " LINES TERMINATED BY  '\n'"
#         " IGNORE 1 LINES ("
#         " AID,"
#         " GI,"
#         " GeneID,"
#         " Accession,"
#         " UniProtKB_ACID);"  % 
#         (os.getcwd(), locfile, TABLE_NAME))
#   except mysql.connector.Error as e:
#     sys.stderr.write("Failed loading data: {}\n".format(e))
#     return
#   print "--%s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])