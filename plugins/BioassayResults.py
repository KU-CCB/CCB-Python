#!/usr/bin/python

import sys
import os
from ftplib import FTP
import gzip
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

TABLE_NAME = "Aid2GiGeneidAccessionUniprot"
remfile = "Aid2GiGeneidAccessionUniprot.gz"
locfile = remfile[:-3]
pcdir  = "pubchem/Bioassay/Extras"
server = "ftp.ncbi.nih.gov"

def update(user, passwd, db):
  print "plugin: %s" % __name__
  print "--downloading files"
  ftp = FTP(server)
  ftp.login() # anonymous
  ftp.cwd(pcdir)
  ftp.retrbinary("RETR %s" % remfile, open("./data/%s" % remfile, 'wb').write)
  ftp.quit()
  print "--unzipping files"
  with gzip.open("%s/data/%s" % (os.getcwd(), remfile), 'rb') as inf:
    with open("%s/data/%s" % (os.getcwd(), locfile), 'w') as outf:
      for line in inf:
        outf.write(line)
  print "--loading %s into table" % remfile
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
        (os.getcwd(), locfile, TABLE_NAME))
  except mysql.connector.Error as e:
    sys.stderr.write("Failed loading data: {}\n".format(e))
    return
  print "--%s complete" % __name__

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.arv[3])