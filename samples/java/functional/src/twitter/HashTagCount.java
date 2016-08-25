/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package twitter;

import java.io.Serializable;

@SuppressWarnings("serial")
public class HashTagCount implements Serializable {
    private String hashTag;
    private Integer count;

    public HashTagCount(String hashTag, Integer count) {
        this.hashTag = hashTag;
        this.count = count;
    }

    /**
     * @return the hashTag
     */
    public String getHashTag() {
        return hashTag;
    }

    /**
     * @param hashTag
     *            the hashTag to set
     */
    public void setHashTag(String hashTag) {
        this.hashTag = hashTag;
    }

    /**
     * @return the count
     */
    public Integer getCount() {
        return count;
    }

    /**
     * @param count
     *            the count to set
     */
    public void setCount(Integer count) {
        this.count = count;
    }

    public String toString() {
        return "{" + hashTag + ": " + Integer.toString(count) + "}";
    }

}