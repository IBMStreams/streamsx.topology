/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package twitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * A container for a hashmap that keeps the top ten of whatever is in the map.
 */
public class Trender {
    // Overall hashmap for hashtags -- maintains the count for each hashtag
    private HashMap<String, Integer> hmap = new HashMap<String, Integer>();

    public HashMap<String, Integer> getMap() {
        return hmap;
    }

    /*
     * Adds a hashtag to the table of hashtags, increasing its count.
     */
    public void add(String s) {
        Integer val;

        // Insert the hashtag into the map.
        if (!hmap.containsKey(s)) {
            val = 1;
            hmap.put(s, val);
        } else {
            val = hmap.get(s) + 1;
            hmap.put(s, val);
        }
    }

    /**
     * Get a hashMap element
     * 
     * @param s
     * @return an element of the hashmap
     */
    public Integer get(String s) {
        return hmap.get(s);
    }

    /**
     * Gets the top ten most frequent hashtags as a list.
     * 
     * @return A ListContainer containing the top ten most frequent hashtags.
     */
    public List<HashTagCount> getTopTen() {
        ArrayList<HashTagCount> topTen = new ArrayList<HashTagCount>();

        // String[] topTen= new String[10];
        Integer minValue = 0;
        for (Entry<String, Integer> eset : hmap.entrySet()) {
            if (eset.getValue() < minValue) {
                continue;
            }
            if (topTen.size() > 10) {
                topTen.remove(0);
            }
            HashTagCount htc = new HashTagCount(eset.getKey(), eset.getValue());
            int idx = Collections.binarySearch(topTen, htc,
                    new Comparator<HashTagCount>() {

                        @Override
                        public int compare(HashTagCount o1, HashTagCount o2) {
                            if (o1.getCount() > o2.getCount())
                                return 1;
                            else if (o1.getCount().equals(o2.getCount()))
                                return 0;
                            else
                                return -1;
                        }
                    });
            if (idx < 0)
                idx = (-1 * idx) - 1;
            topTen.add(idx, htc);
            minValue = topTen.get(0).getCount();
        }
        // Possible for the list to have 11 items
        if (topTen.size() > 10)
            topTen.remove(0);
        return topTen;
    }

}
