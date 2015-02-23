#!/usr/bin/python

import sys
import os
import ConfigParser
import subprocess
from ftplib import FTP
import gzip 
from datetime import datetime, timedelta
import plugins.store.parsesdf as sdf
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

__all__ = ["update"]
plugin = __name__[__name__.index('.')+1:] if __name__ != "__main__"  else "main"
cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")
server            = "ftp.ncbi.nih.gov"
pubchemDir        = "pubchem/Compound/Weekly"
localArchiveDir   = "%s/compounds" % cfg.get('default','tmp')
localUngzippedDir = "%s/compounds/ungzipped" % cfg.get('default','tmp')
localParsedDir    = "%s/compounds/parsed" % cfg.get('default','tmp')
fullDataFile      = "%s/compounds/parsed/fullData.csv" % cfg.get('default','tmp')

def _makedirs(dirs):
  for d in dirs:
    if not os.path.exists(d):
      os.makedirs(d)

def _getUpdatedFolder(ftp):
  ftp.cwd(pubchemDir)
  for folderDate in ftp.nlst():
    sevenDaysAgo = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    if sevenDaysAgo < folderDate:
      return "%s/SDF" % folderDate
  return None

def _downloadFolder(ftp, folder):
  ftp.cwd(folder)
  files = ftp.nlst()
  for i in range(0, len(files)):
    sys.stdout.write("\r> downloading files (%s/%s)" % (i, len(files)))
    sys.stdout.flush()
    # ignore the readme file
    if files[i].find("README") < 0:
      localFilePath = "%s/%s" % (localArchiveDir, files[i])
      ftp.retrbinary("RETR %s" % files[i], open(localFilePath, 'wb').write)
    if i > 5:
      break
  print ""

def _extractArchives():
  path,_,files = next(os.walk(localArchiveDir))
  for f in files:
    with gzip.open("%s/%s" % (localArchiveDir, f), 'rb') as inf:
      with open("%s/%s" % (localUngzippedDir, f[:-3]), 'w') as outf:
        for line in inf:
          outf.write(line)

def _processFiles():
  path,_,files = next(os.walk(localUngzippedDir))
  for f in files:
    sdf.parseFile("%s/%s" % (path, f), 
                  "%s/%s" % (localParsedDir, f))

def _loadMysqlTable(user, passwd, db):
  path,_,files = next(os.walk(localParsedDir))
  for f in files:
    cnx = mysql.connector.connect(user=user, passwd=passwd, db=db, client_flags=[ClientFlag.LOCAL_FILES])
    cursor = cnx.cursor()
    try:
      query = (
        "LOAD DATA LOCAL INFILE '%s/%s'"
        " REPLACE"
        " INTO TABLE `Compounds`"
        " FIELDS TERMINATED BY '^'"
        " LINES TERMINATED BY '\n' ("
        " PUBCHEM_COMPOUND_CID,"
        " PUBCHEM_COMPOUND_CANONICALIZED,"
        " PUBCHEM_CACTVS_COMPLEXITY,"
        " PUBCHEM_CACTVS_HBOND_ACCEPTOR,"
        " PUBCHEM_CACTVS_HBOND_DONOR,"
        " PUBCHEM_CACTVS_ROTATABLE_BOND,"
        " PUBCHEM_CACTVS_SUBSKEYS,"
        " PUBCHEM_IUPAC_INCHI,"
        " PUBCHEM_IUPAC_INCHIKEY,"
        " PUBCHEM_EXACT_MASS,"
        " PUBCHEM_MOLECULAR_FORMULA,"
        " PUBCHEM_MOLECULAR_WEIGHT,"
        " PUBCHEM_OPENEYE_CAN_SMILES,"
        " PUBCHEM_OPENEYE_ISO_SMILES,"
        " PUBCHEM_CACTVS_TPSA,"
        " PUBCHEM_MONOISOTOPIC_WEIGHT,"
        " PUBCHEM_TOTAL_CHARGE,"
        " PUBCHEM_HEAVY_ATOM_COUNT,"
        " PUBCHEM_ATOM_DEF_STEREO_COUNT,"
        " PUBCHEM_ATOM_UDEF_STEREO_COUNT,"  
        " PUBCHEM_BOND_DEF_STEREO_COUNT,"
        " PUBCHEM_BOND_UDEF_STEREO_COUNT,"
        " PUBCHEM_ISOTOPIC_ATOM_COUNT,"
        " PUBCHEM_COMPONENT_COUNT,"
        " PUBCHEM_CACTVS_TAUTO_COUNT);" %
        (path, f))
      cursor.execute(query)
      cnx.commit()
    except mysql.connector.Error as e:
      sys.stderr.write("x failed loading data: {}\n".format(e))

def update(user, passwd, db):
  print "plugin: %s" % plugin
  print "> creating space on local machine"
  _makedirs([localArchiveDir, localUngzippedDir, localParsedDir])
  print "> checking for updated files"
  ftp = FTP(server)
  ftp.login() # anonymous
  folder = _getUpdatedFolder(ftp)
  if not folder is None:
    print "> found files updated on %s " % folder[:folder.index('/')] 
    _downloadFolder(ftp, folder)
  ftp.quit()
  print "> extracting files"
  _extractArchives()
  print "> processing files"
  _processFiles()
  print "> loading files into table"
  _loadMysqlTable(user, passwd, db) 
  print "> %s complete" % plugin

if __name__=="__main__":
  if len(sys.argv) < 4:
    print "Usage: %s <username> <password> <database>" % __file__
    sys.exit()
  update(sys.argv[1], sys.argv[2], sys.argv[3])