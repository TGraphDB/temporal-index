package org.act.temporal_index;

import java.io.File;
import java.io.IOException;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.serializer.SerializerLong;

public class AppTest 
{
	
	private static int nodeNum = 1010;
	private static int valueNum = 10;
	private static int timeNum = 1000;
	
	
	static{
		File dir = new File("F:/WorkSpace/temporal-index/target/test/");
		System.out.println(dir.getName());
		if(!dir.exists())
			dir.mkdirs();
	}
	
	public static void main(String[] args){
		
//		DB db = DBMaker.fileDB("test")
//				.fileLockDisable()
//				.concurrencyDisable()
//				.make();
//		BTreeMap<Long, Long> map = db.treeMap("map")
//				.keySerializer(new SerializerLong())
//				.valueSerializer(new SerializerLong())
//				.create();
//		map.put(1l, 1l);
//		db.close();
//		DBMaker.fileDB("test")
//				.fileDeleteAfterClose()
//				.make().close();
		
		TemporalIndex index = new TemporalIndex("F:/WorkSpace/temporal-index/target/test");
		index.init();
		for( int t = 0; t<timeNum; t++ ){
			for( int i = 0; i<nodeNum; i++ ){
				index.insert(t, t*5-1, new byte[]{(byte)(i%valueNum)}, (long)i);
			}
		}
		index.close();
	}
}
