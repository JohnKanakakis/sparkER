package spark.preprocessing;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.aksw.limes.core.io.config.KBInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.HDFSUtils;

public class DataParser {

	public static Logger logger = LoggerFactory.getLogger(DataParser.class);
	
	public static JavaPairRDD<String, Tuple2<String, String>> run(JavaPairRDD<LongWritable, Text> data,
																int partitions,final Broadcast<byte[]> kbB){
			
		
		final KBInfo kb = (KBInfo)HDFSUtils.deserialize(kbB.getValue());
		
			return
					data.mapToPair(
			        		new PairFunction<Tuple2<LongWritable,Text>,String, Tuple2<String,String>>(){
	
			        			private static final long serialVersionUID = 1L;
	
			        			private  List<String> toTriple(Text t,List<String> buffer){
			        				
			        				
			        				
			        				byte[] b = t.getBytes();
			        				int pos1 = t.find("<");
			        				
			        				if(pos1 != 0) return null;
			        				
			        				int pos2 = t.find(">",pos1+1);
			        				
			        				if(pos2 == -1) return null;
			        				
			        				String s = null;
			        				try {
			        					s = Text.decode(b, pos1+1, pos2-pos1-1);
			        				} catch (CharacterCodingException e) {
			        					// TODO Auto-generated catch block
			        					//e.printStackTrace();
			        					return null;
			        				}
			        				
			        				pos1 = t.find("<",pos2+1);
			        				if(pos1 == -1) return null;
			        				
			        				pos2 = t.find(">",pos1+1);
			        				if(pos2 == -1) return null;
			        				
			        				String p = null;
			        				try {
			        					p = Text.decode(b, pos1+1, pos2-pos1-1);
			        				} catch (CharacterCodingException e) {
			        					
			        					return null;
			        				}
			        				
			        				
			        				String o = null;
			        				pos1 = pos2;
			        				while(t.charAt(pos1) != 9 && t.charAt(pos1) != ' ' && pos1 < t.getLength()){// 9 equals to tab
			        					pos1++;
			        				}
			        				pos1++;
			        				
			        				if(pos1 >= t.getLength())
			        					return null;
			        				
			        				if(t.charAt(pos1) == 34){// 34 equals to "\""
			        					try {
			        						o = Text.decode(b, pos1,t.getLength()-pos1-2);
			        					} catch (CharacterCodingException e) {
			        						return null;
			        					}
			        					
			        				}else if (t.charAt(pos1) == 60){// 60 equals to "<"
			        					//object is URI
			        					pos2 = t.find(">",pos1+1);
			        					if(pos2 == -1){
			        						return null;
			        					}
			        					try {
			        						o = Text.decode(b, pos1+1,pos2 - pos1 -1);
			        					} catch (CharacterCodingException e) {
			        						return null;
			        					}
			        					
			        				}else{
			        					return null;
			        				}
			        				
			        				if(s != null && p != null && o != null){
			        					buffer.add(s);
			        					buffer.add(p);
			        					buffer.add(o);
			        					return buffer;
			        				}else
			        					return null;
			        			}
			        			
			        			@Override
			        			public Tuple2<String, Tuple2<String, String>> call(Tuple2<LongWritable,Text> record) throws Exception {
			        				
			        				List<String> triple;
			        				ArrayList<String> buffer = new ArrayList<String>(3);
			        				
			    					triple = toTriple(record._2, buffer);
			    					if(triple == null){
			    						logger.error("invalid triple "+record._2.charAt(0));
			    						
			    						return new Tuple2<String,Tuple2<String,String>>("",
			        							new Tuple2<String,String>("",""));
			    					}
			    					if(triple.size() != 3){
			    						logger.error("malformed triple "+record._2);
			    						
			    						return new Tuple2<String,Tuple2<String,String>>("",
			        							new Tuple2<String,String>("",""));
			    						
			    					}	
			    					
			    					String subject = triple.get(0);
			    					String property = triple.get(1);
			    					String object = triple.get(2);
			    					
			    					
			    					if(DataFilter.containedInXMLConfiguration(property,object,kb)){
			    						
			    						if(property.equals("http://purl.org/dc/terms/identifier") || 
				    							property.equals("http://purl.org/dc/elements/1.1/identifier")){
				    						
				    						String DOI = object;
				    						DOI = DOIParser.parseDOI(DOI);
				    						
				    						if(DOI == null){
				    							return new Tuple2<String,Tuple2<String,String>>("",
					        							new Tuple2<String,String>("",""));
				    						}
				    						
				    						object = "\""+DOI+"\"";
				    					}
			    						return new Tuple2<String,Tuple2<String,String>>(subject,
				    							new Tuple2<String,String>(property,object));
			    					
			    					}
			    					
			    					return new Tuple2<String,Tuple2<String,String>>("",
		        							new Tuple2<String,String>("",""));
			    					

			        			}
			           
			                })
			                .filter(new Function<Tuple2<String,Tuple2<String,String>>,Boolean>(){
	
									private static final long serialVersionUID = 1L;
						
									@Override
									public Boolean call(Tuple2<String, Tuple2<String, String>> resource) throws Exception {
										
										return !resource._1.equals("");
									}
		                	});
		}
}
