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
    "`SID` int(15) NOT NULL DEFAULT '0',"
    "`CID` int(15) NOT NULL DEFAULT '0',"
    "`Activity_Outcome` varchar(15) DEFAULT NULL,"
    "`Activity_Score` tinyint(4) DEFAULT NULL,"
    "`Test_URL` text,"
    "`Test_Comment` text,"
    "`Active_Concentration` int(11) DEFAULT NULL,"
    "`TIDData` text,"
    "`AID` int(15) NOT NULL DEFAULT '0',"
    "`ActivityID` int(11) NOT NULL AUTO_INCREMENT,"
    "PRIMARY KEY (`SID`,`CID`,`AID`),"
    "UNIQUE KEY `TESTID` (`ActivityID`),"
    "KEY `cid_aid_idx` (`CID`,`AID`),"
    "KEY `aid_idx` (`AID`),"
    "KEY `activity_outcome_idx` (`Activity_Outcome`)"
    ") ENGINE=MyISAM AUTO_INCREMENT=227601852 DEFAULT CHARSET=latin1 ROW_FORMAT=FIXED"
  ),

  'Compounds': (
    "CREATE TABLE `Compounds` ("
    "`CID` int(11) NOT NULL,"
    "`NumAtoms` smallint(5) unsigned NOT NULL,"
    "`NumBonds` mediumint(8) unsigned NOT NULL,"
    "`Formula` text,"
    "`SMILES` text,"
    "`NumHeavyAtoms` tinyint(3) unsigned NOT NULL,"
    "`NumResidues` tinyint(3) unsigned NOT NULL,"
    "`NumRotors` tinyint(3) unsigned NOT NULL,"
    "`NumConformers` tinyint(3) unsigned NOT NULL,"
    "`Energy` smallint(5) unsigned NOT NULL,"
    "`MolecularWeight` float(6,3) NOT NULL,"
    "`ExactMass` float(6,3) NOT NULL,"
    "`Charge` tinyint(4) NOT NULL,"
    "`SpinMultiplicity` tinyint(4) NOT NULL,"
    "`Dimension` tinyint(4) NOT NULL,"
    "`IsChiral` tinyint(4) NOT NULL,"
    "`IsEmpty` tinyint(4) NOT NULL,"
    "PRIMARY KEY (`CID`),"
    "UNIQUE KEY `CID` (`CID`)"
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
