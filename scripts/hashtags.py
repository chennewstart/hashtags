from config import DBSERVER
from cassandra.cluster import Cluster
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import SVC
from sklearn.preprocessing import LabelBinarizer
from sklearn.decomposition import PCA
from sklearn.cross_decomposition import CCA
import nltk.stem
english_stemmer = nltk.stem.SnowballStemmer('english')

# PCA, CCA
# 2-gram
# stem
# CCA(n_components=2)

class StemmedTfidfVectorizer(TfidfVectorizer):
	def cleantweet(self, tweet):
		return " ".join([x for x in tweet.lower().replace("#", '').split() if not x.startswith("@") and not x.startswith("http")])

	def build_analyzer(self):
		analyzer = super(StemmedTfidfVectorizer, self).build_analyzer()
		return lambda doc: (english_stemmer.stem(w) for w in analyzer(self.cleantweet(doc)))

if __name__ == "__main__":
	cluster = Cluster([DBSERVER])
	session = cluster.connect()
	# session.execute("CREATE KEYSPACE IF NOT EXISTS TweetsXiaohu WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
	session.execute("USE TweetsXiaohu")
	# session.execute("DROP TABLE IF EXISTS Tweet")
	rows = session.execute("SELECT text, hashtags FROM Tweet limit 1000")
	X, Y = [], []
	for row in rows:
		X.append(row.text)
		Y.append([x.lower() for x in row.hashtags])
	vectorizer = StemmedTfidfVectorizer(min_df=1, stop_words='english', decode_error='ignore')
	# print(vectorizer)

	X = vectorizer.fit_transform(X).toarray()
	# print '40', X
	# print type(X)
	Y_indicator = LabelBinarizer().fit(Y).transform(Y)
	cca = CCA(n_components = 100, max_iter=10)
	cca.fit(X, Y_indicator)
	X = cca.transform(X)
	# print '45', X
	# print type(X)
	classif = OneVsRestClassifier(SVC(kernel='linear'))
	classif.fit(X, Y)

	for row in rows:
		# row = rows[0]
		# print vectorizer.transform([row.text]).toarray()
		# print cca.predict(vectorizer.transform([row.text]).toarray())
		transformed = vectorizer.transform([row.text]).toarray()
		# print '55', transformed
		ccad = cca.transform(transformed)
		# print '57', ccad
		predicts = classif.predict(ccad)
		if len(predicts) > 0:
			print row.text, predicts


