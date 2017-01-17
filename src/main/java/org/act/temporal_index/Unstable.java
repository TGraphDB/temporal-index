package org.act.temporal_index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.act.temporal_index.tree.MemIndex;
import org.act.temporal_index.tree.TimeIndex;
import org.act.temporal_index.util.Filename;
import org.act.temporal_index.util.LogReader;
import org.act.temporal_index.util.LogWriter;
import org.act.temporal_index.util.Logs;
import org.act.temporal_index.util.Slice;
import org.act.temporal_index.util.VersionEdit;


public class Unstable {

	
	private SortedMap<Integer, IndexMetaData> indexs;
	private String dbDir;
	private Stable stable;
	
	public Unstable(String dbDir, Stable stable){
		indexs = new TreeMap<Integer, IndexMetaData>();
		for( int i = 1; i<=4; i++){
			indexs.put(i, null);
		}
		this.dbDir = dbDir;
		this.stable = stable;
	}
	
	public void take(MemIndex memIndex, int endTime) {
		
		for( int i = 1; i <= 4; i++ ){
			if( indexs.get(i) == null ){
				IndexMetaData metaData = new IndexMetaData(i, 0, memIndex.getStartTime(), endTime);
				indexs.put(i, metaData);
				TimeIndex timeIndex = new TimeIndex(dbDir, "un"+i);
				memIndex.flush(timeIndex,endTime);
				return;
			}
		}
		TimeIndex timeIndex = new TimeIndex(dbDir, "s"+stable.getNewNumber());
		this.stable.add(timeIndex.mergeFrom(this.indexs,stable.getNewNumber()));
		for( int i = 1; i<=4; i++ )
			indexs.put(i, null);
		IndexMetaData metaData = new IndexMetaData(1, 0, memIndex.getStartTime(), endTime);
		indexs.put(1, metaData);
		TimeIndex timeIndex2 = new TimeIndex(dbDir, "un1");
		memIndex.flush(timeIndex2, endTime);
	}

	public List<Long> get(int startTime, int endTime, byte[] value) {
		if( endTime < startTime )
			return null;
		List<Long> toret = new LinkedList<>();
		for( IndexMetaData metaData : indexs.values() ){
			if(metaData != null){
				toret.addAll(new TimeIndex(dbDir, "un"+metaData.getNumber()).
						get(Math.max(startTime, metaData.getSmallest()), Math.min(endTime, metaData.getLargest()), value));
			}
		}
		toret.addAll(this.stable.get(startTime,endTime,value));
		return toret;
	}

	public boolean remove(int startTime, int endTime, byte[] value, long id) {
		boolean toret = true;
		for(IndexMetaData metaData : indexs.values()){
			if( startTime>= metaData.getSmallest() && startTime <= metaData.getLargest() ){
				TimeIndex timeIndex = new TimeIndex(dbDir, "un"+metaData.getNumber());
				toret = toret & timeIndex.remove(startTime, startTime, value, id);
				timeIndex.close();
			}
			if( endTime >= metaData.getSmallest() && endTime <= metaData.getLargest() ){
				TimeIndex timeIndex = new TimeIndex(dbDir, "un"+metaData.getNumber());
				toret = toret & timeIndex.remove(endTime, endTime, value, id);
				timeIndex.close();
			}
		}
		return toret & this.stable.remove(startTime,endTime,value,id);
	}

	public void init() throws IOException {
		String logFileName = Filename.logFileName( 0 );
        File logFile = new File( this.dbDir + "/" + logFileName );
        if( !logFile.exists() )
        {
            return;
        }
        FileInputStream inputStream = new FileInputStream( logFile );
        FileChannel channel = inputStream.getChannel();
        LogReader logReader = new LogReader( channel, null, false, 0 );
        for( Slice logRecord = logReader.readRecord(); logRecord != null; logRecord = logReader.readRecord() )
        {
            VersionEdit edit = new VersionEdit( logRecord );
            for( Entry<Integer,IndexMetaData> entry : edit.getNewFiles().entries() )
            {
                this.indexs.put((int)entry.getValue().getNumber(), entry.getValue());
            }
        }
        inputStream.close();
        channel.close();
        Files.delete( logFile.toPath() );
        this.stable.init();
	}

	public void close() throws IOException {
		LogWriter writer = Logs.createLogWriter( dbDir, false );
        VersionEdit edit = new VersionEdit();
        for( IndexMetaData meta : this.indexs.values() )
        {
            if( null != meta )
                edit.addFile( 0, meta );
        }
        writer.addRecord( edit.encode(), true );
        writer.close();
        this.stable.close();
	}

}
