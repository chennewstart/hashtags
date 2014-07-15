URL=http://localhost:8983/solr/update/json
curl $URL -H 'Content-type:application/json' -d '
[
  {
    "id" : "MyTestDocument",
    "title" : "This is just a test"
  }
]'
curl "$URL?commit=true"

with open("part-r-00000") as f:
	for line in f:
