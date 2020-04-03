from flask import Flask, render_template, redirect, url_for, request, session, Response
import datetime
import time
import re
import json
import os
from subprocess import Popen, PIPE, STDOUT

app = Flask(__name__)
app.secret_key = os.urandom(12)

@app.route('/sparse/<key>')
def home(key):
	page = request.args.get('option', default = False, type = str)
	try:
		key = "%x" % int(key)
	except ValueError:
		pass
	search = re.compile(r'stripe.*key.*([A-F0-9].20$)')
	search_err = re.compile('(err=.*|SCAL_SPARSE.*)')
	listkey = []
	cmd = "python /home/website/bin/sparsecmd.py --conf /home/website/bin/sfused.conf dump %s" % key
	p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = p.communicate()
	retkey  = stdout.split('\n')

	if page == "full":
		if stderr:	return json.dumps(stderr)
		else:		return json.dumps(retkey)

	for i in retkey:
		if search.search(i):
			key = i.split()[3]
			listkey.append(key)
	if stderr:
		err = search_err.search(stderr).group(0)
		listkey=[err]
	if len(listkey) == 0:
		listkey=["empty"]
	return json.dumps(listkey)

