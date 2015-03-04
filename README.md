# Database
KU ITTC CCB Python Database scripts

#### Structure
```sh
.
├── config.cfg     # configuration file for database scripts
├── create-ccb.py  # creates the database
├── update-ccb.py  # top-level database update script
├── lib/           # third-party software
├── plugins/       # database table update scripts (table name = script name)
├── Makefile
└── README.md
```

#### Usage
```sh
make
python create-ccb.py username password
python update-ccb.py username password
```

##### To clean up the directory tree (remove junk files and submodules)
```sh
make clean
```

##### To restore up the directory tree (remove junk files and submodules)
```sh
make restore
```

**You can also run each plugin file directly to update a single database table**

```sh
cd plugins/
python Bioassays.py username password ccb
```
