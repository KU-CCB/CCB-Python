#!/usr/bin/python
"""
TODO
- Move column types into config file
"""

import sys

import socket
# mysql.connector is only installed on the acf cluster for python2.6 but we need
# to use python2.7+ because the 'with' directive is not backwards compatible
# with v2.6.*
if socket.gethostname()[-6:] == "ku.edu":
	sys.path.append('/usr/lib/python2.6/site-packages/')
import ConfigParser
import mysql.connector
from mysql.connector import errorcode

if len(sys.argv) < 3:
  print "CCBDB.create-ccb  The KU CCB Database creation script"
  print "  For more information, see https://github.com/KU-CCB/CCBDB.git"
  print ""
  print "  For issues see https://github.com/KU-CCB/CCBDB/issues or"
  print "  contact Kendal Harland <kendaljharland@gmail.com>"
  print ""
  print "  Usage: %s <username> <password>" % __file__
  sys.exit()

cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")

# s for each of the table columns restricted to a specific set of values 
# default (assay|substance|compound)_id to 0 if missing
TYPES = {
  'assay_id':          "MEDIUMINT UNSIGNED DEFAULT 0",
  'substance_id':      "INT UNSIGNED DEFAULT 0",
  'compound_id':       "INT UNSIGNED DEFAULT 0",
  'gene_id':           "INT UNSIGNED", 
  'gi':                "INT UNSIGNED", # Global identifier
  'ncbi_accession':    "varchar(15)", # Needs check (NCBI Accession)
  'uniprot_kb':        "varchar(15)", # Needs check (Uniprot KB Accession/Identifier)
  'activity_outcome':  "ENUM('Inactive','Active','Inconclusive','Unspecified','Probe')",
  'activity_score':    "SMALLINT", # Needs check, encountered values in range (-128, 128) could be tinyint
  'activity_URL':      "TEXT",
  'assay_description': "MEDIUMTEXT", # Needs check, might have to be larger depending on description format (JSON, ASNX, etc.)
  'assay_comment':     "MEDIUMTEXT", # Needs check
}

TABLES = {
  'Aid2GiGeneidAccessionUniprot': (
    "CREATE TABLE `Aid2GiGeneidAccessionUniprot` ("
    "`assay_id`       " + TYPES["assay_id"] + " NOT NULL,"
    "`gi`             " + TYPES["gi"] + " NOT NULL,"
    "`gene_id`        " + TYPES["gene_id"] + ","
    "`ncbi_accession` " + TYPES["ncbi_accession"] + ","
    "`uniprot_kb`     " + TYPES["uniprot_kb"] + ","
    "PRIMARY KEY (`assay_id`,`gi`,`gene_id`)" # WTF IS THIS SUPPOSED TO BE?
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1;"
  ),

  'Bioassays': (
    "CREATE TABLE `Bioassays` ("
    "`assay_id`          " + TYPES["assay_id"] + ","
    "`substance_id`      " + TYPES["substance_id"] + ","
    "`activity_outcome`  " + TYPES["activity_outcome"] + ","
    "`activity_score`    " + TYPES["activity_score"] + ","
    "`activity_URL`      " + TYPES["activity_URL"] + ","
    "`assay_comment`     " + TYPES["assay_comment"] + ","
    "PRIMARY KEY (`assay_id`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 ROW_FORMAT=FIXED"
  ),

  'Substance_id_compound_id': (
    "CREATE TABLE `substance_id_compound_id` ("
    "`substance_id` " + TYPES["substance_id"] + ","
    "`compound_id`  " + TYPES["compound_id"] + ","
    "PRIMARY KEY (`substance_id`),"
    "KEY compound_id_idx (`compound_id`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1;"
  ),

  'Assay_id_assay_description': (
    "CREATE TABLE `Assay_id_assay_description` ("
    "`assay_id`          " + TYPES["assay_id"] + ","
    "`assay_description` " + TYPES["assay_description"] + ","
    "PRIMARY KEY (`assay_id`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 ROW_FORMAT=FIXED"
  )
}

cnx = mysql.connector.connect(user=sys.argv[1], password=sys.argv[2])
cursor = cnx.cursor()

try: # Create the database
  cursor.execute("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET '%s'" % 
    (cfg.get('default','database'), cfg.get('default','charset')))
  cursor.execute("use %s;" % cfg.get('default','database'))
  print "> Database %s successfully created" % cfg.get('default','database')
except mysql.connector.Error as e:
  sys.stderr.write("x Failed creating database: %s\n" % e)
  sys.exit()

try: # Set database options
  # Make mysql respect our types
  cursor.execute("set sql_mode='STRICT_ALL_TABLES';") 
  print "> Database %s configuration set successfully"
except mysql.connector.Error as e:
  sys.stderr.write("x Failed configuring database: %s\n" % e)
  sys.exit()

try: # Access the databse
  cursor.execute("use %s" % cfg.get('default','database'))
except mysql.connector.Error as e:
  sys.stderr.write("x Failed to access database: %s\n" % e)
  sys.exit()

# Create the tables
for table, query in TABLES.iteritems():
  try:
    cursor.execute(query)
    print "> Table %s successfully created" % table
  except mysql.connector.Error as e:
    sys.stderr.write("x Failed creating table: %s\n" % e)
