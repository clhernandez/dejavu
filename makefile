configure:
	rm -rf venv
	python3 -m venv venv
	$(shell source venv/bin/activate)
	pip install -r requirements.txt