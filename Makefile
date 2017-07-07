
.PHONY: depinstall clean doc sdist test

clean:
	rm -rf sdc/rabbit/doc/html
	rm -v dist/sdc-rabbit-python-*.tar.gz

depinstall:
	pip3 install -r requirements.txt

doc: depinstall
	pip install .[docbuild]
	sphinx-build sdc/rabbit/doc sdc/rabbit/doc/html

sdist: doc
	python setup.py sdist

test: depinstall
	pip3 install .[dev]
	flake8 .
	python -m unittest discover sdc
