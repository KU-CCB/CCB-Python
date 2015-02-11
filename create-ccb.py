#!/usr/bin/python

import sys
import ConfigParser
import mysql.connector
from mysql.connector import errorcode

if len(sys.argv) < 3:
  print "Usage: %s <username> <password>" % __file__
  sys.exit()

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

TABLES = {
  'Aid2GiGeneidAccessionUniprot': (
    "CREATE TABLE `Aid2GiGeneidAccessionUniprot` ("
    "`AID` bigint(20) NOT NULL,"
    "`GI` int(11) NOT NULL,"
    "`GeneID` int(11) NOT NULL,"
    "`Accession` varchar(15) DEFAULT NULL,"
    "`UniProtKB_ACID` varchar(15) DEFAULT NULL,"
    "PRIMARY KEY (`AID`,`GI`,`GeneID`),"
    "KEY `geneid_index` (`GeneID`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1;"
  ),

  'Bioassays': (
    "CREATE TABLE `Bioassays` ("
    "`AID` int(15) NOT NULL DEFAULT '0',"
    "`SID` int(15) NOT NULL DEFAULT '0',"
    "`CID` int(15) NOT NULL DEFAULT '0',"
    "`Activity_Outcome` varchar(15) DEFAULT NULL,"
    "`Activity_Score` tinyint(4) DEFAULT NULL,"
    "`Activity_URL` text DEFAULT NULL,"
    "`Comment` text,"
    "PRIMARY KEY (`SID`,`CID`,`AID`),"
    "KEY `cid_aid_idx` (`CID`,`AID`),"
    "KEY `sid_aid_idx` (`CID`,`AID`),"
    "KEY `aid_idx` (`AID`),"
    "KEY `activity_outcome_idx` (`Activity_Outcome`)"
    ") ENGINE=MyISAM AUTO_INCREMENT=227601852 DEFAULT CHARSET=latin1 ROW_FORMAT=FIXED"
  ),

  'Compounds': (
    "CREATE TABLE `Compounds` ("
    " `PUBCHEM_COMPOUND_CID` int(11) NOT NULL,"
    " `PUBCHEM_COMPOUND_CANONICALIZED` int,"
    " `PUBCHEM_CACTVS_COMPLEXITY` int,"
    " `PUBCHEM_CACTVS_HBOND_ACCEPTOR` int',"
    " `PUBCHEM_CACTVS_HBOND_DONOR` int,"
    " `PUBCHEM_CACTVS_ROTATABLE_BOND` int,"
    " `PUBCHEM_CACTVS_SUBSKEYS` text',"
    " `PUBCHEM_IUPAC_INCHI` text,"
    " `PUBCHEM_IUPAC_INCHIKEY` text,"
    " `PUBCHEM_EXACT_MASS` float,"
    " `PUBCHEM_MOLECULAR_FORMULA` text,"
    " `PUBCHEM_MOLECULAR_WEIGHT` float,"
    " `PUBCHEM_OPENEYE_CAN_SMILES` text,"
    " `PUBCHEM_OPENEYE_ISO_SMILES` text,"
    " `PUBCHEM_CACTVS_TPSA` float,"
    " `PUBCHEM_MONOISOTOPIC_WEIGHT` float,"
    " `PUBCHEM_TOTAL_CHARGE` int,"
    " `PUBCHEM_HEAVY_ATOM_COUNT` int,"
    " `PUBCHEM_ATOM_DEF_STEREO_COUNT` int,"
    " `PUBCHEM_ATOM_UDEF_STEREO_COUNT` int,"
    " `PUBCHEM_BOND_DEF_STEREO_COUNT` int,"
    " `PUBCHEM_BOND_UDEF_STEREO_COUNT` int,"
    " `PUBCHEM_ISOTOPIC_ATOM_COUNT` int,"
    " `PUBCHEM_COMPONENT_COUNT` int,"
    " `PUBCHEM_CACTVS_TAUTO_COUNT` int,"
    " PRIMARY KEY (`PUBCHEM_COMPOUND_CID`),"
    " UNIQUE KEY `PUBCHEM_COMPOUND_CID` (`PUBCHEM_COMPOUND_CID`)"
    ") ENGINE=InnoDB DEFAULT CHARSET=latin1"
  ),

  'ProteinTargets': (
    "CREATE TABLE `ProteinTargets` ("
    "`GI` int(11) NOT NULL,"
    "`FASTASequence` text,"
    "`dbName` varchar(5) DEFAULT NULL,"
    "`dbID` varchar(20) DEFAULT NULL,"
    "PRIMARY KEY (`GI`),"
    "UNIQUE KEY `GeneID` (`GI`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1"
  )
}

cnx = mysql.connector.connect(user=sys.argv[1], password=sys.argv[2])
cursor = cnx.cursor()

# Create the database
try:
  cursor.execute(
    "CREATE DATABASE %s DEFAULT CHARACTER SET '%s'" % 
      (cfg.get('default','db'), cfg.get('default','charset')))
  print "> Database %s successfully created" % cfg.get('default','db')
except mysql.connector.Error as e:
  sys.stderr.write("x Failed creating database: {}\n".format(e))
  sys.exit()

# Access the databse
try:
  cursor.execute("use %s" % cfg.get('default','db'))
except mysql.connector.Error as e:
  sys.stderr.write("x Failed to access database: {}\n".format(e))
  sys.exit()

# Create the tables
for table, query in TABLES.iteritems():
  try:
    cursor.execute(query)
    print "> Table %s successfully created" % table
  except mysql.connector.Error as e:
    sys.stderr.write("x Failed creating table: {}\n".format(e))
