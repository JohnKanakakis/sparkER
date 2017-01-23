package spark.preprocessing;



import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aksw.limes.core.io.config.KBInfo;
//import org.apache.jena.rdf.model.Property;
//import org.apache.jena.vocabulary.RDF;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.DatasetManager;
import spark.HDFSUtils;


/**
 * The EntityFilter filters the data according to the LIMES configuration
 * @author John Kanakakis
 *
 */
public class DataFilter {
	
	public static Logger logger = LoggerFactory.getLogger(DataFilter.class);
	public final static String TYPE_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";//RDF.type;
	
	public static Broadcast<byte[]> kbB;
	
	public static boolean containedInXMLConfiguration(String property, String object,KBInfo kb) {
		
		//KBInfo kb = (KBInfo)HDFSUtils.deserialize(kbB.getValue());
		HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
		configProperties.add(TYPE_PROPERTY.toString());
		
		if (property.equals(TYPE_PROPERTY)
				&& object.equals(kb.getClassRestriction())) {
			//System.out.println("entity class filtering "+property +" "+object);
			return true;
		}

		
		if(!property.equals(TYPE_PROPERTY) && configProperties.contains(property)){
			return true;
		}
		
		return false;
	}
	
	
	
	private static JavaPairRDD<String, Tuple2<String, String>> filterByEntityClass(
			JavaPairRDD<String, Tuple2<String, String>> triplesRDD,
			final String entityClassRestriction) {
		
		return triplesRDD
				.filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(
							Tuple2<String, Tuple2<String, String>> triple)
							throws Exception {
						
						String property = triple._2._1;
						String object = triple._2._2;

						
						
						if (property.equals(TYPE_PROPERTY)
								&& object.equals(entityClassRestriction)) {
							//System.out.println("entity class filtering "+property +" "+object);
							return true;
						}

						return false;
					}

				});

	}
	
	
	
	
	private static JavaPairRDD<String, Tuple2<String, String>> filterByEntityProperties(JavaPairRDD<String, Tuple2<String, String>> triplesRDD,
																					   final HashSet<String> configProperties)
	{
		return
		triplesRDD.filter(new Function<Tuple2<String, Tuple2<String, String>>,Boolean>(){

			private static final long serialVersionUID = 1L;
			
			
			private Boolean hasValidDOI(String line){
				String DOI;
				//String line = "10.4404/hystrix 7.1 2 4074 ";
				line = line.trim();
		       
				//Decode DOI strings before applying the regex
				String result = "";
				try {
					result = java.net.URLDecoder.decode(line, "UTF-8");
				} catch (UnsupportedEncodingException | java.lang.IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
		       
				//Pattern for DOI
//			       String pattern="10.\\d{4}/\\d{3}/.*";
				String pattern="10.\\d{4}/.+(=?).+?(?=[0-9])";

				// Create a Pattern object
				Pattern r = Pattern.compile(pattern);

				// Create matcher object.
				Matcher m = r.matcher(result);
				if (m.find( )) {
		          System.out.println("Found value1: " + m.group() );
		          DOI = m.group();
		          
		          //cleaning openAIRE's DOIs which have stripped from dashes
		          if(DOI.contains(" ")){
		           DOI= DOI.replaceAll(" ","-");
		           //System.out.println("DOI is: " +DOI );
		           
		          }
		          
		          return true;
		      	}else {
		          //Handle No matches here
		      		return false;
		      	}
			}
			
			
			@Override
			public Boolean call(Tuple2<String, Tuple2<String, String>> triple) throws Exception {
				// TODO Auto-generated method stub
			
				String property = triple._2._1;
				String object = triple._2._2;
				
				
				if(!property.equals(TYPE_PROPERTY) && configProperties.contains(property)){
					
					/*if(property.equals("http://purl.org/dc/terms/identifier") || 
							property.equals("http://purl.org/dc/elements/1.1/identifier")){
						
						if(hasValidDOI(object)){
							return true;
						}else{
							return false;
						}
					}*/
					return true;
				}

				return false;
			}
			
		});
		
	}
	
    
    
   
	/**
	 * @param triplesRD : the triples RDD
	 * @param kbB : 
	 * the broadcasted KBInfo object which holds the information of
	 * the entities (type class, properties etc) involved in the linking task 
	 * @return the filtered data in the form of (r_id, [info]) 
	 */
	public static JavaPairRDD<String, Tuple2<String, String>> filterByLIMESConfiguration(
			JavaPairRDD<String, Tuple2<String, String>> triplesRDD, 
			final Broadcast<byte[]> kbB) {
		
		final KBInfo kb = (KBInfo)HDFSUtils.deserialize(kbB.getValue());
		
		final HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
		configProperties.add(TYPE_PROPERTY.toString());
		
		/*System.out.println(configProperties);
		System.exit(0);*/
		JavaPairRDD<String, Tuple2<String, String>> triplesRDD_1 = filterByEntityProperties(triplesRDD,configProperties);
		
		triplesRDD_1 = triplesRDD_1.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>,String, Tuple2<String, String>>(){

			private static final long serialVersionUID = 1L;

			private String changeDOI(String line){
				String DOI;
				//String line = "10.4404/hystrix 7.1 2 4074 ";
				line = line.trim();
		       
				//Decode DOI strings before applying the regex
				String result = "";
				try {
					result = java.net.URLDecoder.decode(line, "UTF-8");
				} catch (UnsupportedEncodingException | java.lang.IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
		       
				//Pattern for DOI
//			       String pattern="10.\\d{4}/\\d{3}/.*";
				//String pattern="10.\\d{4}/.*";
				String pattern = "10.\\d{4}/.+(=?).+?(?=[0-9])";
				
				// Create a Pattern object
				Pattern r = Pattern.compile(pattern);

				// Create matcher object.
				Matcher m = r.matcher(result);
				if (m.find( )) {
		          System.out.println("Found value1: " + m.group() );
		          DOI = m.group();
		          
		          //cleaning openAIRE's DOIs which have stripped from dashes
		          if(DOI.contains(" ")){
		           DOI= DOI.replaceAll(" ","-");
		           //System.out.println("DOI is: " +DOI );
		           
		          }
		          
		          return DOI;
		      	}else {
		          //Handle No matches here
		      		return null;
		      	}
			}
			@Override
			public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				
				String property = t._2._1;
				
				
				if(property.equals("http://purl.org/dc/terms/identifier") || 
						property.equals("http://purl.org/dc/elements/1.1/identifier")){
					String DOI = t._2._2;
					DOI = DOI.replace("\"", "");
					String new_DOI = changeDOI(DOI);
					
					if(new_DOI == null){
						return new Tuple2<String, Tuple2<String, String>>("",new Tuple2<String,String>("",""));
					}
					
					new_DOI = "\""+new_DOI+"\"";
					
					Tuple2<String, Tuple2<String, String>> new_t = 
							new Tuple2<String, Tuple2<String, String>>(t._1,new Tuple2<String,String>(property,new_DOI));
					
					return new_t;
				}else{
					
					return t;
				}
				
				
				
				
			}
			
		});
		
		JavaPairRDD<String, Tuple2<String, String>> triplesRDD_2 = filterByEntityClass(triplesRDD,kb.getClassRestriction());
		
		return triplesRDD_1.union(triplesRDD_2);
		
	}




	
}


