.PHONY: setup clean restore git links

PYPUG_REMOTE=https://github.com/KU-CCB/PyPUG.git
PYPUG_LOCAL=./lib/PyPUG

setup: git links
	@echo "> Done!"

clean:
	rm -rf *.pyc 
	rm -rf plugins/*.pyc 
	rm -rf plugins/store/*.pyc

restore: clean
	git rm -rf $(PYPUG_LOCAL)

git:
	git submodule add --force $(PYPUG_REMOTE) $(PYPUG_LOCAL)
	git submodule init
	git submodule update
	
links:
	ln -sf ../$(PYPUG_LOCAL)/pypug.py ./plugins/pypug.py
