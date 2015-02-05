import sys
import mysql.connector
from mysql.connector import errorcode

if len(sys.argv) < 3:
  print "Usage: %s <username> <password>" % __file__
  sys.exit()

DEFAULT_CHARSET = "utf8"
DB_NAME = "ccb"
TABLES = {
  'AID2GIGeneID': (
    "CREATE TABLE `AID2GIGeneID` ("
    "`AID` bigint(20) NOT NULL,"
    "`GI` int(11) NOT NULL,"
    "`GeneID` int(11) NOT NULL,"
    "`Accession` varchar(15) DEFAULT NULL,"
    "`UniProtKB_ACID` varchar(15) DEFAULT NULL,"
    "PRIMARY KEY (`AID`,`GI`,`GeneID`),"
    "KEY `geneid_index` (`GeneID`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1;"
  ),

  'BioassayResults': (
    "CREATE TABLE `BioassayResults` ("
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

  'Bioassays': (
    "CREATE TABLE `Bioassays` ("
    "`AID` bigint(20) NOT NULL DEFAULT '0',"
    "`Assay_type` varchar(20) DEFAULT NULL,"
    "`Target` varchar(300) DEFAULT NULL,"
    "PRIMARY KEY (`AID`),"
    "UNIQUE KEY `AID` (`AID`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1"
  ),

  'ChemicalAttributes': (
    "CREATE TABLE `ChemicalAttributes` ("
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

try:
  cursor.execute(
    "CREATE DATABASE %s DEFAULT CHARACTER SET '%s'" % 
      (DB_NAME, DEFAULT_CHARSET))
  print("> Database %s successfully created" % DB_NAME)
except mysql.connector.Error as e:
  print("! Failed creating database: {}".format(e))
  sys.exit()

try:
  cursor.execute("use {}".format(DB_NAME))
except mysql.connector.Error as e:
  print("! Failed to access database: {}".format(e))
  sys.exit()

for table, query in TABLES.iteritems():
  try:
    cursor.execute(query)
    print("> Table %s successfully created" % table)
  except mysql.connector.Error as e:
    print("! Failed creating table: {}".format(e))
