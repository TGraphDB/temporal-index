package org.act.temporal_index.tree;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.act.temporal_index.IndexMetaData;
import org.act.temporal_index.util.TimeTreeMergeIterator;
import org.act.temporal_index.util.TreeUtil;
import org.act.temporal_index.util.ValueTreeMergeIterator;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.serializer.SerializerByteArray;
import org.mapdb.serializer.SerializerInteger;
import org.mapdb.serializer.SerializerLongArray;
import org.mapdb.serializer.SerializerString;
import kotlin.Pair;

public class TimeIndex {
	
	private String dbDir;
	private String name;
	private DB db;
	private BTreeMap<byte[], String> valueTree;
	private BTreeMap<Integer, long[]> timeTree;
	
	public TimeIndex(String dbDir, String name) {
		this.dbDir = dbDir;
		this.name = name;
		this.db = DBMaker.fileDB(dbDir+"/"+this.name)
				.concurrencyDisable()
				.fileLockDisable()
				.make();
	}
	
	public List<Long> get(int start, int end, byte[] value) {
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.counterEnable()
				.open();
		String timeName = valueTree.get(value);
		if( timeName == null )
			return null;
		timeTree = db.treeMap(timeName  + "timetree" )
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
		this.db.close();
		return new LinkedList<Long>(toret);
	}
	
	public void insert(int startTime, int endTime, byte[] value, long id) {
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.createOrOpen();
		String name = valueTree.get(value);
		if(null == value)
			valueTree.put(value, new String(value));
		timeTree = db.treeMap(new String(value) + "timetree")
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.createOrOpen();
		long[] value1 = TreeUtil.getTimePoint(startTime, timeTree);
		long[] value2 = TreeUtil.getTimePoint(endTime+1, timeTree);
		Set<Long> set = new HashSet<>();
		for( long l : value1 )
			set.add(l);
		set.add(id);
		long[] value3 = new long[set.size()];
		int index = 0;
		for( long l : set ){
			value3[index++] = l;
		}
		timeTree.put(startTime, value3);
		timeTree.put(endTime+1, value2);
		this.db.close();
	}

	public void flushValueTree(Iterator<Entry<byte[], String>> valueIterator) {
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

	public void flushTimeTree(String treeName, Iterator<Entry<Integer, long[]>> timeIterator) {
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


	public IndexMetaData mergeFrom(SortedMap<Integer, IndexMetaData> indexs, int newNumber) {
		List<TimeIndex> timeIndexs = new LinkedList<>();
		int start = Integer.MAX_VALUE;
		int end = -1;
		for(IndexMetaData metaData : indexs.values() ){
			TimeIndex timeIndex = new TimeIndex(dbDir, "un"+metaData.getNumber());
			timeIndexs.add(timeIndex);
			start = Math.min(metaData.getSmallest(), start);
			end = Math.max(metaData.getLargest(), end);
		}
		//--开始value树的合并
		List<Iterator<Entry<byte[], String>>> valueIteratorList = new LinkedList<>();
		for(TimeIndex timeIndex : timeIndexs){
			valueIteratorList.add(timeIndex.getValueTree().entryIterator());
		}
		Iterator<Pair<byte[], String>> mergeIterator = new ValueTreeMergeIterator(valueIteratorList,new SerializerByteArray());
		valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.createFrom(mergeIterator);
		//--结束value树的合并
		//--开始time树合并
		Iterator<String> values = valueTree.valueIterator();
		while( values.hasNext() ){
			String value = values.next();
			List<Iterator<Entry<Integer, long[]>>> timeIteratorList = new LinkedList<>();
			for( TimeIndex timeIndex : timeIndexs ){
				BTreeMap<Integer, long[]> timeTree = timeIndex.getTimeTree(value);
				if( timeTree != null ){
					timeIteratorList.add(timeTree.entryIterator());
				}
			}
			Iterator<Pair<Integer, long[]>> timeMergeIterator = new TimeTreeMergeIterator( timeIteratorList, new SerializerInteger() );
			this.timeTree = db.treeMap( value  + "timetree" )
					.keySerializer(new SerializerInteger())
					.valueSerializer(new SerializerLongArray())
					.valuesOutsideNodesEnable()
					.counterEnable()
					.createFrom(timeMergeIterator);
		}
		//--结束time树合并
		for( TimeIndex timeIndex : timeIndexs ){
			timeIndex.close();
			timeIndex.clear();
		}
		this.db.close();
		IndexMetaData indexMetaData = new IndexMetaData(newNumber, 0, start, end);
		return indexMetaData;
	}

	private BTreeMap<Integer, long[]> getTimeTree(String treeName) {
		try{
		timeTree = db.treeMap( treeName  + "timetree" )
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.open();
		}catch (Exception e) {
			return null;
		}
		return timeTree;
	}

	private BTreeMap<byte[], String> getValueTree() {
		this.valueTree = db.treeMap("valuetree")
				.keySerializer(new SerializerByteArray())
				.valueSerializer(new SerializerString())
				.open();
		return this.valueTree;
	}

	public void clear() {
		DBMaker.fileDB(dbDir+"/"+this.name)
				.fileDeleteAfterClose()
				.make().close();
	}

	public void close() {
		this.db.close();
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
//		Iterator<Entry<Integer, long[]>> iterator = timeTree.entryIterator(startTime, true, endTime, true);
//		while( iterator.hasNext() ){
//			Entry<Integer, long[]> entry = iterator.next();
//			deleteList.add(entry.getKey());
//		}
		deleteList.add(startTime);
		deleteList.add(endTime+1);
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

	public void flushFrom(MemIndex time) {
		Iterator<Entry<byte[], String>> valueIterator = time.valueTreeIterator();
		this.flushValueTree(valueIterator);
		valueIterator = time.valueTreeIterator();
		while( valueIterator.hasNext() ){
			 Entry<byte[], String> entry = valueIterator.next();
			 this.flushTimeTree(entry.getValue(), time.timeTreeIterator(entry.getValue()));
		}
	}

	public Iterator<Entry<byte[], String>> valueTreeIterator() {
		return this.valueTree.entryIterator();
	}

	public Iterator<Entry<Integer, long[]>> timeTreeIterator(String timeTreeName) {
		timeTree = db.treeMap(timeTreeName)
				.keySerializer(new SerializerInteger())
				.valueSerializer(new SerializerLongArray())
				.valuesOutsideNodesEnable()
				.counterEnable()
				.open();
		return timeTree.entryIterator();
	}
	
}
















