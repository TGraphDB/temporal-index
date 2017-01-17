package org.act.temporal_index;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.act.temporal_index.tree.MemIndex;
import org.act.temporal_index.tree.TimeIndex;

public class TemporalIndex {

	private String dbDir;
	private MemIndex memIndex;
	private Unstable unstable;
	private int memBoundary;
	
	public TemporalIndex( String dbDir ){
		this.dbDir = dbDir;
		new File(this.dbDir).mkdirs();
		memIndex = new MemIndex();
		unstable = new Unstable(this.dbDir, new Stable(this.dbDir));
	}
	
	
	public void init(){
		if( new File("memdisc").exists()){
			TimeIndex discMem = new TimeIndex(dbDir, "memdisc");
			this.memIndex.flushFrom( discMem );
			discMem.clear();
		}
		try {
			unstable.init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		TimeIndex discMem = new TimeIndex(dbDir, "memdisc");
		discMem.flushFrom(this.memIndex);
		discMem.close();
		try {
			unstable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void insert( int startTime, int endTime, byte[] value, long id){
		if(memIndex.size()>=2048*16 && startTime>memBoundary){
			unstable.take(memIndex,startTime-1);
			memIndex.clear(startTime);
			memBoundary = startTime;
		}
		else{
			memIndex.insert(startTime, endTime, value, id);
			memBoundary = Math.max(memBoundary, startTime);
		}
	}
	
	
	public long[] get( int startTime, int endTime, byte[] value ){
		Set<Long> toret = new HashSet<>();
		toret.addAll(this.memIndex.get(Math.max(startTime, this.memIndex.getStartTime()), endTime, value));
		toret.addAll(this.unstable.get(startTime,Math.min(endTime, this.memIndex.getStartTime()-1),value));
		long[] ret = new long[toret.size()];
		int index = 0;
		for(long l : toret ){
			ret[index++] = l;
		}
		return ret;
	}
	
	public boolean remove( int startTime, int endTime, byte[] value, long id){
//		if( endTime >= this.memIndex.getStartTime() ){
//			boolean toret = this.memIndex.remove(Math.max(startTime, this.memIndex.getStartTime()), endTime, value, id);
//			if( startTime < this.memIndex.getStartTime() ){
//				toret = toret & this.unstable.remove(startTime,Math.min(endTime, this.memIndex.getStartTime()-1), value, id);
//			}
//			return toret;	
//		}
//		else{
//			return this.unstable.remove(startTime,endTime, value, id);
//		}
		
		if(startTime>=this.memIndex.getStartTime()){
			return this.memIndex.remove(startTime, endTime, value, id);
		}
		if( endTime >= this.memIndex.getStartTime() ){
			return this.memIndex.remove(endTime, endTime, value, id) && this.unstable.remove(startTime, startTime, value, id);		
		}
		return this.unstable.remove(startTime, endTime, value, id);
		
	}
}
