package com.zerodb.prkeyrotation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

import com.zerodb.crypto.BBS98Engine;

public class KeyRotation {
	static void ReEncrypt(LocatedFileStatus status) {
		
	}
	
	public static void main(String[] args) {
		
		try{
			String rekey = args[0];
			String param = args[1];
			int lower = 0, upper = 2000;
			Configuration conf = new Configuration();
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
			Path path = null;
			RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, true);
			int index = 0;
			
			//get dfsclient 
	        Field dfsfield = DistributedFileSystem.class.getDeclaredField("dfs");
	        dfsfield.setAccessible(true);
	        DFSClient dfsclient = (DFSClient)dfsfield.get(fs);
	        
	        BBS98Engine engine = new BBS98Engine();
			while (iter.hasNext()) {
				index++;
				LocatedFileStatus status = iter.next();
				if (index >= lower && index < upper) {
					//ReEncrypt(status);
					HdfsFileStatus hdfsstatus = dfsclient.getFileInfo(status.getPath().toString());
					FileEncryptionInfo feinfo = hdfsstatus.getFileEncryptionInfo();
					//engine.reEncrypt(param, rekey, feinfo.getEncryptedDataEncryptionKey());
				}
			}
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
