clean:
	rm -rf *.pyc 
	rm -rf plugins/*.pyc 
	rm plugins/store/*.pyc

git:
	@echo "> Initializing git submodules..."
	@git submodule init
	@git submodule update
	
links:
	@echo "> Creating symlinks for libraries"
	@ln -sf ./lib/PyPUG/pypug ./plugins/pypug.py

all: git links
	@echo "> Done!"
