package org.act.temporal_index.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mapdb.serializer.SerializerByteArray;

import kotlin.Pair;

public class ValueTreeMergeIterator implements Iterator<Pair<byte[], String>> {

	
	private Map<Integer, Iterator<Entry<byte[], String>>> iterators;
	private Map<Integer,Entry<byte[], String>> buffer;
	private SerializerByteArray serializer;
	private byte[] lastKey;
	
	public ValueTreeMergeIterator(List<Iterator<Entry<byte[], String>>> valueIteratorList,
			SerializerByteArray serializerByteArray) {
		this.serializer = serializerByteArray;
		int index = 0;
		this.iterators = new HashMap<>();
		for(Iterator<Entry<byte[], String>> iterator : valueIteratorList ){
			this.iterators.put(index++, iterator);
		}
		this.buffer = new HashMap<>();
		for(Entry<Integer, Iterator<Entry<byte[], String>>> entry : iterators.entrySet() ){
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
	public Pair<byte[], String> next() {
		int minIndex = -1;
		Entry<byte[], String> minEntry = null;
		for(Entry<Integer, Entry<byte[], String>> entry : buffer.entrySet() ){
			if(minEntry == null || this.serializer.compare(minEntry.getKey(), entry.getValue().getKey() ) > 0){
				minEntry = entry.getValue();
				minIndex = entry.getKey();
			}
		}
		Map<Integer,Entry<byte[], String>> tempBuffer = new HashMap<>(buffer);
		for(Entry<Integer, Entry<byte[], String>> entry : tempBuffer.entrySet() ){
			if( this.serializer.compare(minEntry.getKey(), entry.getValue().getKey() ) == 0){
				if(iterators.get(entry.getKey()).hasNext()){
					buffer.put(entry.getKey(), iterators.get(entry.getKey()).next());
				} else{
					buffer.remove(entry.getKey());
				}
			}
		}
		if(this.lastKey != null && this.serializer.compare(this.lastKey, minEntry.getKey()) ==0 ){
			if(this.hasNext())
				return next();
		}
		this.lastKey = minEntry.getKey();
		return new Pair<byte[], String>(minEntry.getKey(), minEntry.getValue());
	}

}
