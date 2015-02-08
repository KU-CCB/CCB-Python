#!/usr/bin/python

import sys
import os
from ftplib import FTP
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

TABLE_NAME = "Aid2GiGeneidAccessionUniprot"
pubchemFile = "Aid2GiGeneidAccessionUniprot.gz"
pubchemDir  = "pubchem/Bioassay/Extras"
localFile = pubchemFile[:-3]
server = "ftp.ncbi.nih.gov"

def update(user, passwd, db):
  print "plugin: %s" % __name__
  print "> downloading files"
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pubchemDir)
  ftp.retrbinary("RETR %s" % pubchemFile, open("./data/%s" % localFile, 'wb').write)
  ftp.quit()
  print "> extracting files"
  with gzip.open("%s/data/%s" % (os.getcwd(), localFile), 'rb') as inf:
    with open("%s/data/%s" % (os.getcwd(), localFile), 'w') as outf:
      for line in inf:
        outf.write(line)
  print "> loading %s into table" % localFile
  cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
  cursor = cnx.cursor()
  try:
    cursor.execute(
        "LOAD DATA LOCAL INFILE '%s/data/%s'"
        " REPLACE"
        " INTO TABLE %s"
        " FIELDS TERMINATED BY '\t'"
        " LINES TERMINATED BY  '\n'"
        " IGNORE 1 LINES ("
        " AID,"
        " GI,"
        " GeneID,"
        " Accession,"
        " UniProtKB_ACID);"  % 
        (os.getcwd(), localFile, TABLE_NAME))
  except mysql.connector.Error as e:
    sys.stderr.write("x Failed loading data: {}\n".format(e))
    return
  print "> %s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])