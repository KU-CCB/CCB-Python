# Database
KU ITTC CCB-Python Database scripts

#### Structure
```sh
.
├── config.cfg    # configuration file for database scripts
├── create-ccb.py # creates the database
├── data/         # data file folder
├── makefile      # junk file management
├── plugins/      # database table update scripts (table name = script name)
│   ├── Aid2GiGeneidAccessionUniprot.py
│   ├── Bioassays.py
│   ├── Compounds.py
│   └── store/    # space for persistent files used by plugins
├── README.md
├── setup.py      # creates necessary files & folders
└── update-ccb.py # top-level database update script
```

#### Usage
```sh
python setup.py
python create-ccb.py username password
python update-ccb.py username password
```

**You can also run each plugin file directly to update a single database table**

```sh
cd plugins/
python Bioassays.py username password 'ccb'
```
