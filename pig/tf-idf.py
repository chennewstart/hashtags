import re, math

@outputSchema("words:bag{t:tuple(word:chararray)}")
def tokenize(text):
	# print "[DEBUG]text:", text
	text = text.lower()
	text = re.sub(r'rt\s', '', text)
	text = re.sub(r'(?m)[\d,]+', '', text)
	# print text
	text = re.sub(r'(?m)https?:\/\/\S+', '', text)
	# print text
	text = re.sub(r'(?m)@\S+', '', text)
	# print text
	tokens = re.split('\W+', text)
	# print tokens
	tokens = set(filter(lambda x : len(x) > 0, tokens))
	words = []
	for token in tokens:
		words.append((token, ))
	return words

if __name__ == "__main__":
	text = 'RT @teenchoicenews: 3 Retweet if you want @LittleMix to perform at the Teen Choice Awards this year! http://t.co/JHsWkAnKkh #TeenChoice)'
	# text = 'RT @teenchoicenews: Retweet if you want @LittleMix to perform at the Teen Choice Awards this year! #TeenChoice http://t.co/JHsWkAnKkh)'
	print tokenize(text)