.PHONY: build up tests flake8 ci tests-with-cov

build:
	python setup.py sdist

upload:
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

