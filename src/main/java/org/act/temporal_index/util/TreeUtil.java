package org.act.temporal_index.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.mapdb.BTreeMap;

public class TreeUtil {

	public static long[] getTimePoint(int startTime, BTreeMap<Integer, long[]> timeTree) {
		Iterator<Entry<Integer, long[]>> iterator = timeTree.entryIterator(startTime, true, null, false);
		if( !iterator.hasNext() ){
			return null;
		}
		Entry<Integer, long[]> entry = iterator.next();
		return entry.getValue();
	}

}
