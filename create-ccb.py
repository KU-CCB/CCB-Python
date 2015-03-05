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
import getopt
import ConfigParser
import mysql.connector
from mysql.connector import errorcode


cfg = ConfigParser.ConfigParser()
cfg.read("config.cfg")


def help():
  print "---------------------------------------------------------"
  print "%s - The %s database update script" % (__file__, cfg.get('default', 'database'))
  print "---------------------------------------------------------"
  print "* For information, see %s." % (cfg.get('repo', 'url'))
  print "* Report issues at %s or " % cfg.get('repo', 'issues')
  print "* contact %s at <%s>" % (cfg.get('author', 'name'), cfg.get('author', 'email'))
  print "Usage: %s <username> <password>" % __file__
  print ""

# Read command line options. If none are present, display help message and exit
shortargs = "hH:u:p:"
longargs  = ["help","hostname=","username=","password="]
opts, args = getopt.getopt(sys.argv[1:], shortargs, longargs)
hostname, username, password, database = None,None,None,cfg.get('default','database')
if len(opts) == 0:
  help()
  sys.exit()

for option, value in opts:
  if option in ("-h", "--help"):
    help()
    sys.exit()
  elif option in ("-u", "--username"):
    username = value
  elif option in ("-p", "--password"):
    password = value
  elif option in ("-H", "--hostname"):
    hostname = value
  else:
    assert False, "unhandled option"

if hostname is None: hostname = "127.0.0.1"


# types for each of the table columns restricted to a specific set of values 
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

cnx = mysql.connector.connect(host=hostname, user=username, password=password)
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
