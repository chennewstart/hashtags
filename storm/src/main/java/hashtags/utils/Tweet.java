package hashtags.utils;

import java.io.Serializable;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         Tweet class that records tweet id, text, vector, hashtags.
 * 
 */
public class Tweet implements Serializable {
	private String ID;
	private String text;
	private SparseVector vector;
	private String hashtags;

	public Tweet(String tweetID, String text) {
		this.text = text;
		this.vector = null;
		ID = tweetID;
	}

	public Tweet(String tweetId) {
		this.vector = null;
		ID = tweetId;
	}

	public void setSparseVector(SparseVector sparseV) {
		this.vector = sparseV;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public SparseVector getSparseVector() {
		return vector;
	}

	public boolean hasEmptyVector() {
		return vector == null;
	}

	public String getID() {
		return ID;
	}

	public void setId(String tweetId) {
		ID = tweetId;
	}

	public String getHashtags() {
		return hashtags;
	}

	public void setHashtags(String hashtags) {
		this.hashtags = hashtags;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String SEPARATOR = "<--->";

		result.append(this.getClass().getName() + " {");
		result.append(ID + SEPARATOR);
		result.append(text + SEPARATOR);
		result.append(hashtags + SEPARATOR);
		result.append(vector);
		result.append("}");

		return result.toString();
	}
}
