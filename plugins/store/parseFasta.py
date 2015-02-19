#!/usr/bin/python
"""
File parser for ProteinTargets table plugin
Input Format: FASTA
"""

import sys
import getopt

def help(): print "parse [-h help] [-i input-file] [-o output-file]\n"

def Error(msg): print "Error:", msg;

def Errorq(msg): print "Error:", msg; sys.exit();

class FASTARecord:
	"""Class for representing protein records in FASTA Format"""
	def __init__(self):
		self.gi = None
		self.database = None
		self.identifier = None
		self.sequence = ""
    
	def __str__(self):
		return "%s %s %s %s" % (
			self.gi, 
			self.database, 
			self.identifier, 
			self.sequence
		)

	def AppendSequence(self, chunk):
		self.sequence = "".join([self.sequence, chunk])

try:
  opts, args = getopt.getopt(sys.argv[1:], 'hi:o:')
except:
  help()

infile, outfile, errfile = "" , "", "parse.error"

for opt, arg in opts:
	if opt in ("-i", "--input-file"):
		infile = arg
	elif opt in ("-o", "--output-file"):
		outfile = arg
	elif opt in ("-h", "--help"):
		help()
		sys.exit()
	else:
		assert False, "unhandled option"

records = []
record = None
parsingSeq = False
idTypes = ("gb","emb","pir","sp","ref","dbj","pdb","tpg")

with open(infile) as inf:
  for line in inf:
		if not parsingSeq: # Look for the header. e.g. >gi|111...
			if '>' in line:
				headers = line.split("|")
				headers[0] = headers[0][1:] # Trim '>'
				record = FASTARecord()
				for i, h in enumerate(headers):
					if h == "gi":
						record.gi = headers[i+1]
					elif h in idTypes:
						record.database = headers[i]
						record.identifier = headers[i+1]
				parsingSeq = True
			else:
				Error(line)
		else: # Parse protein sequence
			if len(line.lstrip().rstrip()) > 0:
				record.AppendSequence(line.lstrip().rstrip())
			else:
				records.append(str(record))
				parsingSeq = False

with open(outfile, "a+") as of:
	for record in records:
		of.write(record)
		of.write('\n')