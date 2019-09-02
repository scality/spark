from flask import Flask, render_template, redirect, url_for, request, session, Response
import datetime
import time
import os

app = Flask(__name__)
app.secret_key = os.urandom(12)

@app.route('/key',methods=["GET","POST"])
def home():
    if request.method == 'GET':
 	key = request.args.get('key')
	cmd = "/home/website/bin/scalkeyarcgen -t arc -C 3 -k 4 -m 2 -s 6 %s" % key
	print cmd
	retkey  = os.popen(cmd).read()
	print retkey
	return render_template('ok.html', retkey=retkey)
		
    else:
	print "KO"
	return render_template('ko.html')

