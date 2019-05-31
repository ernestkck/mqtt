default: venv/bin/activate
	venv/bin/python -m pip install paho-mqtt


run: venv
	venv/bin/python client.py

