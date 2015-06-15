/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package twitter;

import java.io.Serializable;
import java.util.List;

/**
 * Currently, generics aren't supported as Stream tuple tupes, so a container is
 * necessary.
 */
@SuppressWarnings("serial")
public class ListContainer implements Serializable {
    private List<HashTagCount> list;

    ListContainer(List<HashTagCount> list) {
        this.list = list;
    }

    public List<HashTagCount> getList() {
        return list;
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
