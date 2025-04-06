all: mypy pylint

mypy:
	mypy process.py

pylint:
	pylint --rcfile=../pylint.rc process.py

.PHONY: all mypy pylint
