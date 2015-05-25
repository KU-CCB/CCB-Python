.PHONY: setup clean restore git links

setup: git links
	@echo "> Done!"

clean:
	rm -rf *.pyc plugins/*.pyc

links:
	ln -sf ../logger/logger.py plugins/logger.py
