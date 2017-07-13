
.PHONY: depinstall clean sdist test

clean:
	rm -rf sdc/rabbit/doc/html
	rm -v dist/sdc-rabbit-python-*.tar.gz

depinstall:
	pip3 install -r requirements.txt

sdist:
	python setup.py sdist

test: depinstall
	pip3 install -r test_requirements.txt
	flake8 .
	python -m unittest discover sdc
