# Real-time #Hashtag Suggestion for Tweets
 
https://bitbucket.org/qanderson

mvn archetype:generate -DgroupId=hastags -DartifactId=hashtags

mvn -e exec:java -Dexec.args="get" -Dexec.mainClass="com.datastax.tutorial.TutorialRunner"
bin/cassandra-cli --host localhost < /path/to/script/npanxx_script.txt

mvn eclipse:clean eclipse:eclipse

# TODO
# test recall, F1
# similar (with hashtag) tweets query
# hashtag suggestion (ranked list or word cloud)
