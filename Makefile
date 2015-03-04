.PHONY: setup clean restore git links

PYPUG_REMOTE=https://github.com/KU-CCB/PyPUG.git
PYPUG_LOCAL=./lib/PyPUG/

setup: git links
	@echo ">Done!"

clean:
	rm -rf *.pyc 
	rm -rf plugins/*.pyc 
	rm -rf plugins/store/*.pyc

restore: clean
	git rm -r $(PYPUG_LOCAL)

git:
	@echo "> Initializing git submodules..."
	@git submodule add $(PYPUG_REMOTE) $(PYPUG_LOCAL)
	@git submodule init
	@git submodule update
	
links:
	@echo "> Creating symlinks for libraries"
	@ln -sf $(PYPUG_LOCAL)/pypug.py ./plugins/pypug.py
