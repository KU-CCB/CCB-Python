# parsesdf.py
# 
# Convert a .SDF file into a space delimited .CSV file.
# Consult this link for further details on compling OpenBabel:
# 
# http://open-babel.readthedocs.org/en/latest/Installation/install.html 
#

import pybel
import sys

print pybel.islocal()

def parseFile(infile, outfile):
  fields = [ 
      'PUBCHEM_COMPOUND_CID',
      'PUBCHEM_COMPOUND_CANONICALIZED',
      'PUBCHEM_CACTVS_COMPLEXITY',
      'PUBCHEM_CACTVS_HBOND_ACCEPTOR', 
      'PUBCHEM_CACTVS_HBOND_DONOR',
      'PUBCHEM_CACTVS_ROTATABLE_BOND',
      'PUBCHEM_CACTVS_SUBSKEYS', 
      'PUBCHEM_IUPAC_INCHI',
      'PUBCHEM_IUPAC_INCHIKEY',
      'PUBCHEM_EXACT_MASS',
      'PUBCHEM_MOLECULAR_FORMULA',
      'PUBCHEM_MOLECULAR_WEIGHT',
      'PUBCHEM_OPENEYE_CAN_SMILES',
      'PUBCHEM_OPENEYE_ISO_SMILES',
      'PUBCHEM_CACTVS_TPSA',
      'PUBCHEM_MONOISOTOPIC_WEIGHT',
      'PUBCHEM_TOTAL_CHARGE',
      'PUBCHEM_HEAVY_ATOM_COUNT',
      'PUBCHEM_ATOM_DEF_STEREO_COUNT',
      'PUBCHEM_ATOM_UDEF_STEREO_COUNT',
      'PUBCHEM_BOND_DEF_STEREO_COUNT',
      'PUBCHEM_BOND_UDEF_STEREO_COUNT',
      'PUBCHEM_ISOTOPIC_ATOM_COUNT',
      'PUBCHEM_COMPONENT_COUNT',
      'PUBCHEM_CACTVS_TAUTO_COUNT'
      ]
  # To see all of these fields use 'dir(mol.data)' below

  mol = pybel.readfile("sdf", infile).next()
  with open(outfile, 'a') as outf:
    outf.write("%s\n" % ("^".join([mol.data[f] for f in fields])) ) 
  #for f in fields:
  # sys.stdout.write("%s: %s\n" % (f, mol.data[f]))

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print "usage: parsesdf <input-file> <output-file>"
    sys.exit()
  parseFile(sys.argv[1], sys.argv[2])
