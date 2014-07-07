import json
from flask import render_template, request, Response, redirect, jsonify
from app import app, host, port, user, passwd, db
from app.helpers.database import con_db

from storm.drpc import DRPCClient

# To create a database connection, add the following
# within your view functions:
# con = con_db(host, port, user, passwd, db)
# drpcclient = DRPCClient("ec2-54-215-207-12.us-west-1.compute.amazonaws.com", 3772)
drpcclient = DRPCClient("localhost", 3772)

# ROUTING/VIEW FUNCTIONS
@app.route('/index')
def index():
	# Renders index.html.
	return render_template('index.html')

@app.route('/')
def demo():
	tweets = request.args.get('tweets')
	return render_template('demo.html', tweets = tweets)

@app.route('/drpc', methods = ['GET', 'POST'])
def drpc():
	if request.method == 'GET':
		text = request.args.get('text')
	else:
		text = request.form['text']		
	print 'drpc called'
	# "RT @Arsenal: 1930 FA Cup was @Arsenal's 1st major trophy. A year later, we won the league http://t.co/M5CmfLoJga"
	print 'text', text
	if text:
		tweets = drpcclient.execute("tweets", text)
	else:
		tweets = []
	# print tweets
	# tweets = drpcclient.execute("tweets", text)
	similartweets = []
	suggestions = []
	for tweet, hashtags, similarity in tweets:
		print tweet, hashtags, similarity
		similartweets.append({"cosine similarity" : similarity, "hashtags" : hashtags, "tweet" : tweet})
		if float(similarity) < 0.2 and len(suggestions) > 5:
			continue
		for hashtag in set(hashtags.split(',')):
			suggestions.append(hashtag)
	# return jsonify({"tweet" : tweet, "hashtags" : hashtags, "cosine similarity" : similarity})
	# return json.dumps(result, sort_keys=True, indent=4, ensure_ascii=False, encoding='utf8')
	return Response(json.dumps({'Hashtag Suggestions' : suggestions, 'Similar Tweets' : similartweets}, indent=4, ensure_ascii=False, encoding='utf8'),  mimetype='application/json')
	# return render_template('home.html', hashtags = suggestions, tweets = result)

@app.route('/home')
def home():
	# Renders home.html.
	return render_template('home.html')

@app.route('/slides')
def about():
	# Renders slides.html.
	return render_template('slides.html')

@app.route('/author')
def contact():
	# Renders author.html.
	return render_template('author.html')

@app.errorhandler(404)
def page_not_found(error):
	return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
	return render_template('500.html'), 500
