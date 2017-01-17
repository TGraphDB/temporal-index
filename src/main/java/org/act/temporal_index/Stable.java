package org.act.temporal_index;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.act.temporal_index.tree.TimeIndex;
import org.act.temporal_index.util.Filename;
import org.act.temporal_index.util.LogReader;
import org.act.temporal_index.util.LogWriter;
import org.act.temporal_index.util.Logs;
import org.act.temporal_index.util.Slice;
import org.act.temporal_index.util.VersionEdit;

public class Stable {

	private String dbDir;
	List<IndexMetaData> indexs;
	
	
	public Stable(String dbDir){
		this.dbDir = dbDir;
		this.indexs = new LinkedList<>();
	}
	
	
	public int getNewNumber() {
		return this.indexs.size()+1;
	}


	public void add(IndexMetaData index) {
		this.indexs.add(index);
	}


	public List<Long> get(int startTime, int endTime, byte[] value) {
		if( endTime < startTime )
			return null;
		List<Long> toret = new LinkedList<>();
		for( IndexMetaData metaData : indexs ){
			if(metaData != null){
				toret.addAll(new TimeIndex(dbDir, "s"+metaData.getNumber()).
						get(Math.max(startTime, metaData.getSmallest()), Math.min(endTime, metaData.getLargest()), value));
			}
		}
		return toret;
	}


	public boolean remove(int startTime, int endTime, byte[] value, long id) {
		boolean toret = true;
		for(IndexMetaData metaData : indexs){
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
		return toret;
	}


	public void init() throws IOException {
		String logFileName = Filename.logFileName( 1 );
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
                this.indexs.add(entry.getValue());
            }
        }
        inputStream.close();
        channel.close();
        Files.delete( logFile.toPath() );
	}


	public void close() throws IOException {
		LogWriter writer = Logs.createLogWriter( dbDir, true );
        VersionEdit edit = new VersionEdit();
        for( IndexMetaData meta : this.indexs )
        {
            if( null != meta )
                edit.addFile( 1, meta );
        }
        writer.addRecord( edit.encode(), true );
        writer.close();
	}


}
