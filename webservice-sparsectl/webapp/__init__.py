from flask import Flask, render_template, redirect, url_for, request, session, Response
import datetime
import time
import os
import re
import json

app = Flask(__name__)
app.secret_key = os.urandom(12)

@app.route('/sparse/<key>')
def home(key):
	search = re.compile('[A-F0-9].20$')
	listkey = []
	cmd = "python /home/website/bin/sparsecmd.py --conf /home/website/bin/sfused.conf dump %s" % key
	print(cmd)
	retkey  = os.popen(cmd).read().split()
	for i in retkey:
		if search.search(i):
			print(i)
			listkey.append(i)
	return json.dumps(listkey)
