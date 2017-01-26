package spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * The class includes methods to access HDFS and to serialize/ deserialize java objects
 * @author John Kanakakis
 *
 */
public class HDFSUtils {

	
	public static InputStream getHDFSFile(String file){
		org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		
		InputStream in = null;
		try {
			hdfs = FileSystem.get(hdfsConf);
			in = hdfs.open(new Path(file));
			
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return in;
		
	}
	
	

	public static void deleteHDFSFile(String file) {
		// TODO Auto-generated method stub
		org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		
		try {
			hdfs = FileSystem.get(hdfsConf);
			hdfs.delete(new Path(file), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
