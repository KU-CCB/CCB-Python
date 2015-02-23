clean:
	rm -rf *.pyc plugins/*.pyc plugins/store/*.pyc
links:
	ln -s /usr/lib64/python2.6/site-packages/pybel.py plugins/store/pybel.py
	ln -s /usr/lib64/python2.6/site-packages/openbabel.py plugins/store/openbabel.py
