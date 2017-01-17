package org.act.temporal_index.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mapdb.serializer.SerializerInteger;

import kotlin.Pair;

public class TimeTreeMergeIterator implements Iterator<Pair<Integer, long[]>> {

	

	private Map<Integer, Iterator<Entry<Integer, long[]>>> iterators;
	private Map<Integer,Entry<Integer, long[]>> buffer;
	private SerializerInteger serializer;
	
	public TimeTreeMergeIterator(List<Iterator<Entry<Integer, long[]>>> timeIteratorList,
			SerializerInteger serializerInteger) {
		this.serializer = serializerInteger;
		int index = 0;
		this.iterators = new HashMap<>();
		for(Iterator<Entry<Integer, long[]>> iterator : timeIteratorList ){
			this.iterators.put(index++, iterator);
		}
		this.buffer = new HashMap<>();
		for(Entry<Integer, Iterator<Entry<Integer, long[]>>> entry : iterators.entrySet() ){
			if(entry.getValue().hasNext()){
				buffer.put(entry.getKey(), entry.getValue().next());
			}
		}
	}

	@Override
	public boolean hasNext() {
		return buffer.size() > 0;
	}

	@Override
	public Pair<Integer, long[]> next() {
		int maxIndex = -1;
		Entry<Integer, long[]> maxEntry = null;
		for(Entry<Integer, Entry<Integer, long[]>> entry : buffer.entrySet() ){
			if(maxEntry == null || this.serializer.compare(maxEntry.getKey(), entry.getValue().getKey() ) > 0){
				maxEntry = entry.getValue();
				maxIndex = entry.getKey();
			}
		}
		if(iterators.get(maxIndex).hasNext()){
			buffer.put(maxIndex, iterators.get(maxIndex).next());
		} else{
			buffer.remove(maxIndex);
		}
		return new Pair<Integer, long[]>(maxEntry.getKey(), maxEntry.getValue());
	}

}
