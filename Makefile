all:
	python2 scripts/process.py

clean:
	rm -r data/* archive/*

.PHONY: clean
