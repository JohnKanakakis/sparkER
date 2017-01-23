package spark;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Utils {

	
	
	public static String eliminateDataTypeFromLiteral(String literal){
		int pos = literal.lastIndexOf("^^");
		if(pos == -1){
			return literal;
		}
		return literal.substring(0,pos);
	}
	
	/*public static final String SEPERATOR = "@@@";
	public static Logger logger = LoggerFactory.getLogger(DataFormatter.class);
	
	public static String getResourceId(String record){
		
		int pos = record.indexOf(",");
		String r_id = record.substring(1, pos);
		return r_id;
	}
	
	public static String[] getResourceInfo(String record){
		int pos = record.indexOf(",");
		record = record.replace(", ", ",");
		String info = record.substring(pos+2,record.length()-2);
		
		return info.split("@@@,|@@@");
	}
	
	
	
	public static List<String> toTriple(String s,List<String> l){
		
		int pos1;
		int pos2;
		String p;
		int cnt = 0;
		//ArrayList<String> triple = new ArrayList<String>();
		boolean validTriple = true;
		while(validTriple){
			pos1 = s.indexOf("<");
			pos2 = s.indexOf(">");
			if(pos1 == -1 || pos2 == -1 || pos2 < pos1){
				if(cnt == 3){
					//System.out.println("valid triple..exiting");
					break;
				}else{
					//System.out.println("invalid triple..exiting");
					pos1 = s.indexOf("\"");
					if(pos1 == -1){
						validTriple = false;
					}else{
						pos2 = s.lastIndexOf('\"');
						p = s.substring(pos1,pos2+1);
						l.add(p);
						//validTriple = true;
						break;
					}
				}
			}else{
				p = s.substring(pos1+1,pos2);
				l.add(p);
				s = s.substring(pos2+1);
				cnt++;
			}
		}
		if(!validTriple){
			
			return null;
		}
		return l;
	}
	*/
	
}

