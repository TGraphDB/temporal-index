package org.act.temporal_index.tree;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.act.temporal_index.util.TreeUtil;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.serializer.SerializerByteArray;
import org.mapdb.serializer.SerializerInteger;
import org.mapdb.serializer.SerializerLongArray;
import org.mapdb.serializer.SerializerString;

import kotlin.Pair;

public class MemIndex {	
	
	private int size = 0;
	
	private DB db;
	private BTreeMap<byte[], String> valueTree;
	private BTreeMap<Integer, long[]> timeTree;
	
	private int startTime = Integer.MAX_VALUE;
	
	public MemIndex() {		
		this.db = DBMaker.memoryDB()
				.concurrencyDisable()
				.make();
	}
	
	public List<Long> get(int start, int end, byte[] value) {
		if( end < start )
			return null;
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.counterEnable()
				.open();
		String timeName = valueTree.get(value);
		if( timeName == null )
			return null;
		timeTree = db.treeMap(timeName)
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.open();
		Set<Long> toret = new HashSet<Long>();
		Iterator<Entry<Integer,long[]>> iterable = timeTree.entryIterator(start, true, end, true);
		while(iterable.hasNext()){
			Entry<Integer, long[]> entry = iterable.next();
			for( long l : entry.getValue() )
				toret.add(l);
		}
		return new LinkedList<Long>(toret);
	}
	
	public int size(){
		return size;
	}
	
	public void clear( int newstartTime ){
		size = 0;
		this.startTime = newstartTime;
	}

	public void insert(int startTime, int endTime, byte[] value, long id) {
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.createOrOpen();
		String name = valueTree.get(value);
		if(null == name)
			valueTree.put(value, new String(value));
		timeTree = db.treeMap(new String(value))
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.createOrOpen();
		long[] value1 = TreeUtil.getTimePoint(startTime, timeTree);
		long[] value2 = TreeUtil.getTimePoint(endTime+1, timeTree);
		Set<Long> set = new HashSet<>();
		if( value1 != null ){
			for( long l : value1 )
				set.add(l);
		}
		set.add(id);
		long[] value3 = new long[set.size()];
		int index = 0;
		for( long l : set ){
			value3[index++] = l;
		}
		timeTree.put(startTime, value3);
		if( value2 != null )
			timeTree.put(endTime+1, value2);
		{
			Iterator<Entry<Integer, long[]>> iterator = timeTree.entryIterator(startTime, false, endTime+1, false);
			Map<Integer, long[]> toInsert = new HashMap<>();
			while(iterator.hasNext()){
				Entry<Integer, long[]> entry = iterator.next();
				Set<Long> set2 = new HashSet<>();
				for( long l : entry.getValue() )
					set2.add(l);
				set2.add(id);
				long[] newValue = new long[set2.size()];
				int index2 = 0;
				for(long l : set2 ){
					newValue[index2++] = l;
				}
				toInsert.put(entry.getKey(), newValue);
			}
			for( Entry<Integer, long[]> entry : toInsert.entrySet() ){
				timeTree.put(entry.getKey(), entry.getValue());
			}
		}
		this.startTime = Math.min(startTime, this.startTime);
		this.size++;
	}
	
	public int getStartTime(){
		return this.startTime;
	}

	public void flush(TimeIndex timeIndex, int endTime) {
		Iterator<Entry<byte[], String>> valueIterator = valueTree.entryIterator();
		timeIndex.flushValueTree(valueIterator);
		valueIterator = valueTree.entryIterator();
		while( valueIterator.hasNext() ){
			 Entry<byte[], String> entry = valueIterator.next();
			 timeTree = db.treeMap(entry.getValue())
						.keySerializer(new SerializerInteger())
						.valueSerializer(new SerializerLongArray())
						.valuesOutsideNodesEnable()
						.counterEnable()
						.open();
			 List<Entry<Integer, long[]>> timeList = new LinkedList<>();
			 Iterator<Entry<Integer, long[]>> timeIterator = timeTree.entryIterator();
			 while(timeIterator.hasNext()){
				 Entry<Integer, long[]> entry2 = timeIterator.next();
				 if( entry2.getKey() <= endTime ){
					 timeList.add(entry2);
				 }
			 }
			 timeIndex.flushTimeTree( entry.getValue(), timeList.iterator());
			 long[] endValue = TreeUtil.getTimePoint(endTime+1, timeTree);
			 if( endValue != null )
				 timeTree.put(endTime+1, endValue);
			 for(Entry<Integer, long[]> time:timeList){
				 timeTree.remove(time.getKey());
			 }
			 System.out.println(timeList.get(0).getKey());
		}
		timeIndex.close();
	}

	public boolean remove(int startTime, int endTime, byte[] value, long id) {
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.open();
		String timeTreeName = valueTree.get(value);
		if( timeTreeName == null )
			return false;
		timeTree = db.treeMap(timeTreeName)
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.open();
		if(!(timeTree.containsKey(startTime)&&timeTree.containsKey(endTime+1)))
			return false;
		Set<Integer> deleteList = new HashSet<>();
		Iterator<Entry<Integer, long[]>> iterator = timeTree.entryIterator(startTime, true, endTime+1, false);
		while( iterator.hasNext() ){
			Entry<Integer, long[]> entry = iterator.next();
			deleteList.add(entry.getKey());
		}
//		deleteList.add(startTime);
//		deleteList.add(endTime+1);
		for(int i : deleteList ){
			Set<Long> set = new HashSet<>();
			for( long l : timeTree.get(i) ){
				set.add(l);
			}
			if(!set.remove(id))
				return false;
			if(set.size()==0)
				timeTree.remove(i);
			else{
				long[] newvalue = new long[set.size()];
				int index = 0;
				for(long l : set){
					newvalue[index++] = l;
				}
				timeTree.put(i, newvalue);
			}
		}
		return true;
	}

	public Iterator<Entry<byte[], String>> valueTreeIterator() {
		return this.valueTree.entryIterator();
	}

	public Iterator<Entry<Integer,long[]>> timeTreeIterator(String timeTreeName) {
		timeTree = db.treeMap(timeTreeName)
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.open();
		return timeTree.entryIterator();
	}

	public void flushFrom(TimeIndex discMem) {
		Iterator<Entry<byte[], String>> valueIterator = discMem.valueTreeIterator();
		this.flushValueTree(valueIterator);
		valueIterator = discMem.valueTreeIterator();
		while( valueIterator.hasNext() ){
			 Entry<byte[], String> entry = valueIterator.next();
			 this.flushTimeTree(entry.getValue(), discMem.timeTreeIterator(entry.getValue()));
		}
	}

	private void flushTimeTree(String treeName, Iterator<Entry<Integer, long[]>> timeIterator) {
		List<Pair<Integer, long[]>> timePair = new LinkedList<>();
		while( timeIterator.hasNext() ){
			Entry<Integer, long[]> entry = timeIterator.next();
			timePair.add(new Pair<Integer, long[]>(entry.getKey(), entry.getValue()));
		}
		timeTree = db.treeMap( treeName + "timetree" )
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.createFrom(timePair.iterator());
	}

	private void flushValueTree(Iterator<Entry<byte[], String>> valueIterator) {
		List<Pair<byte[], String>> valuePair = new LinkedList<>();
		while( valueIterator.hasNext() ){
			 Entry<byte[], String> entry = valueIterator.next();
			 valuePair.add(new Pair<byte[], String>(entry.getKey(), entry.getValue()));
		}
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.createFrom(valuePair.iterator());
	}
	
}













