/**
 * xattr.cpp
 * Author: Kendal Harland (kendaljharland@gmail.com)
 * 
 * Convert a .SDF file into a space delimited .CSV file.
 * Consult this link for further details on compling OpenBabel:
 * 
 *	http://open-babel.readthedocs.org/en/latest/Installation/install.html	
 *
 * The following are data sets that need to be stored in a cross-reference 
 * table. The reason is that there are too many elements in these sets of 
 * data for us to just to store them as columns in the ChemicalAttribute Table.
 * For the time being, they have been left out.
 *
 *	- Chirality information for each molecule.
 *	- Ring atom information
 *	- Bond aromaticity information
 *	- Conformer energies
 *	- The torsion between each quadruple of atoms in the given molecule
 *	- The angle between each triple of atoms in the given molecule
 *	- The spaced formula (because the molecular formula is already present)
 *	- The set of all atomic coordinates
 *	- any data from OBMol::FindChilren()
 *	- The largest fragment of the molecule
 *	- various others...
 */

#include <iostream>
#include <strings.h>
#include <openbabel/obconversion.h>
#include <openbabel/mol.h>
#include <openbabel/shared_ptr.h>

// Data separator for output file
#define DELIMITER ' '

int main(int argc, char *argv[])
{
	char errbuf[256];

	if (argc < 2) {
		std::cout
		<< "Usage: xattr [InputFileName]\n"
		<< "Descr: Convert a .SDF file into a .CSV file containing a subset of each\n"
		<< "       chemical's attributes.\n\n";
		return 0;
	}
	
	// Ensure input file is valid. The input file MUST be an MDL type file.
	std::ifstream ifs(argv[1]);
	if (!ifs) {
		bzero(errbuf, 256);
		sprintf(errbuf, "Cannot open input file: %s", argv[1]);
		std::cerr << errbuf << std::endl;
		exit(-1);
	}

	/**
	 * Create converter and molecule object. We don't call SetOutFormat for the 
	 * OBConversion object because we aren't outputting our data in a standard
	 * format. Our target is a space delimited file that update_chem_attr.sh can
	 * read into the mysql Database. This can be adjusted as needed. */
	OpenBabel::OBMol mol;
	OpenBabel::OBConversion conv;
	conv.SetInStream(&ifs);
		
	// As stated before we are working solely with sdf files for now
	OpenBabel::OBFormat *inputFormat = conv.FormatFromExt(argv[1]);
	if (!inputFormat || !conv.SetInFormat(inputFormat)) {
		bzero(errbuf, 256);
		sprintf(errbuf, "Could not find input format for file: %s", argv[1]);
		std::cerr << errbuf << std::endl;
		exit(-1);
	}

	/**
	 * Ensure ouput file is a valid file. It is recommended that this be a .CSV
	 * or .TXT file. */
	std::ofstream chemos(argv[2], std::ios::app);
	if (!chemos) {
		bzero(errbuf, 256);
		sprintf(errbuf, "Cannot open output file: %s", argv[2]);
		std::cerr << errbuf << std::endl;
		exit(-1);
	}

	/**
 	 * Convert the molecular data into a delimited file that update.sh
	 * can use. */
	while (conv.Read(&mol)) {
		chemos
		/* Title (CID)         */	<< mol.GetTitle() << DELIMITER
		/* No. of atoms        */	<< mol.NumAtoms() << DELIMITER
		/* No. of bonds        */	<< mol.NumBonds() << DELIMITER
		/* Chemical formula    */	<< mol.GetFormula() << DELIMITER
		/* No. of heavy atoms  */	<< mol.NumHvyAtoms() << DELIMITER
		/* No. of residues     */	<< mol.NumResidues() << DELIMITER
		/* No. of rotors       */	<< mol.NumRotors() << DELIMITER
		/* No. of Conformers   */	<< mol.NumConformers() << DELIMITER
		/* Energy              */	<< mol.GetEnergy() << DELIMITER
		/* Molecular weight    */	<< mol.GetMolWt(true) << DELIMITER
		/* Exact Mass          */	<< mol.GetExactMass(true) << DELIMITER
		/* Total Charge        */	<< mol.GetTotalCharge() << DELIMITER
		/* Spin Multiplicity   */	<< mol.GetTotalSpinMultiplicity() << DELIMITER
		/* Dimension           */	<< mol.GetDimension() << DELIMITER
		/* Is Chiral?          */	<< mol.IsChiral() << DELIMITER
		/* Is Empty?           */	<< mol.Empty() << DELIMITER	
		<< std::endl;
	}
	
	return 0;
} 
