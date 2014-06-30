package hashtags.utils;

import java.io.Serializable;

public class Tweet implements Serializable {
	private long ID;
	private String body;
	private SparseVector vector;
	private String hashtags;

	public Tweet(long tweetID, String body) {
		this.body = body;
		this.vector = null;
		ID = tweetID;
	}

	public Tweet(long tweetId) {
		this.vector = null;
		ID = tweetId;
	}

	public void setSparseVector(SparseVector sparseV) {
		this.vector = sparseV;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public SparseVector getSparseVector() {
		return vector;
	}

	public boolean hasEmptyVector() {
		return vector == null;
	}

	public long getID() {
		return ID;
	}

	public void setId(long tweetId) {
		ID = tweetId;
	}

	public String getHashtags() {
		return hashtags;
	}

	public void setHashtags(String hashtags) {
		this.hashtags = hashtags;
	}
	
	@Override public String toString() {
	    StringBuilder result = new StringBuilder();
	    String SEPARATOR = "<--->";

	    result.append(this.getClass().getName() + " {");
	    result.append(ID + SEPARATOR);
	    result.append(body + SEPARATOR);
	    result.append(hashtags + SEPARATOR );
//	    result.append(vector);
	    result.append("}");

	    return result.toString();
	  }
}
