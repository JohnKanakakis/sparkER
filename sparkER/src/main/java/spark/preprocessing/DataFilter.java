package spark.preprocessing;



import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
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
import spark.SparkUtils;


/**
 * The EntityFilter filters the data according to the LIMES configuration
 * @author John Kanakakis
 *
 */
public class DataFilter {
	
	public static Logger logger = LoggerFactory.getLogger(DataFilter.class);
	public final static String TYPE_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";//RDF.type;
	protected static final String TYPE_PROPERTY_PREFIX = "rdf:type";
	
	public static Broadcast<byte[]> kbB;
	
	public static boolean containedInXMLConfiguration(String property, String object,KBInfo kb) {
		
		//KBInfo kb = (KBInfo)HDFSUtils.deserialize(kbB.getValue());
		HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
		configProperties.add(TYPE_PROPERTY.toString());
		
		if (property.equals(TYPE_PROPERTY)
				&& object.equals(kb.getClassRestriction())) {
			//logger.info("entity class filtering "+property +" "+object);
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
							//logger.info("entity class filtering "+property +" "+object);
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
		          logger.info("Found value1: " + m.group() );
		          DOI = m.group();
		          
		          //cleaning openAIRE's DOIs which have stripped from dashes
		          if(DOI.contains(" ")){
		           DOI= DOI.replaceAll(" ","-");
		           //logger.info("DOI is: " +DOI );
		           
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
	public static JavaPairRDD<String, Set<Tuple2<String, String>>> ensureLIMESConfiguration(
			JavaPairRDD<String, Set<Tuple2<String, String>>> entitiesRDD, 
			final Broadcast<byte[]> kbB) {
		
		final KBInfo kb = (KBInfo)SparkUtils.deserialize(kbB.getValue());
		
		
		entitiesRDD = entitiesRDD.filter(new Function<Tuple2<String, Set<Tuple2<String, String>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> entity)
					throws Exception {
				
				Set<Tuple2<String, String>> poPairs = entity._2;
				
				logger.info("entity class filtering "+kb.getClassRestriction());
				for(Tuple2<String, String> po : poPairs){
					String property = po._1;
					String object = po._2;
					logger.info("entity class filtering "+property +" "+object);
					if (property.equals(TYPE_PROPERTY)
							&& object.equals(kb.getClassRestriction())) {
						
						return true;
					}
				}
				return false;
			}
			
		});
		return entitiesRDD;
		
	}



	public static JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> applyAllPropertiesFilter(
							JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> records,
							final Broadcast<byte[]> kbB,
							Broadcast<HashMap<String, String>> prefixIndex_B) 
	{
		final KBInfo kb = (KBInfo)SparkUtils.deserialize(kbB.getValue());
		
		final HashMap<String, String> prefixIndex = prefixIndex_B.getValue();
		final HashMap<String, String> invertedPrefixIndex = DatasetManager.invertPrefixIndex(prefixIndex);
		
		Function<Tuple2<String, Set<Tuple2<String, String>>>, Boolean> allPropertiesFilter = 
				new Function<Tuple2<String, Set<Tuple2<String, String>>>, Boolean>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> entity)
					throws Exception 
					{
						List<String> configProperties = kb.getProperties();
						
						configProperties.add(TYPE_PROPERTY_PREFIX);
					
						Set<Tuple2<String, String>> poPairs = entity._2;
						boolean entityHasAllProperties = true;
						
						
						/*logger.info("=============== config properties =============");
						logger.info(configProperties.toString());
						logger.info("===============================================");
						
						logger.info("------------------------------------------------------------------------------------");
						logger.info("subject:"+entity._1);*/
						
						for(Tuple2<String, String> po : poPairs){
							String property = po._1;
							
							
							String propertyWithPrefix = DatasetManager.shrinkURI(property,invertedPrefixIndex);
							
							if(!kb.getProperties().contains(propertyWithPrefix)){
								entityHasAllProperties = false;
								//logger.info(propertyWithPrefix);
								break;
							}
						}
						
						/*if(entityHasAllProperties){
							logger.info("--------------------------- OK ---------------------------------------------------------");
							logger.info("subject:"+entity._1);
							logger.info("----------entityHasAllProperties = "+entityHasAllProperties+"-------------------------");
						}*/
						
						
						return entityHasAllProperties;
					}
			
		};

		return records.filter(allPropertiesFilter);
	}




	
}


/*logger.info(configProperties);
System.exit(0);
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
//	       String pattern="10.\\d{4}/\\d{3}/.*";
		//String pattern="10.\\d{4}/.*";
		String pattern = "10.\\d{4}/.+(=?).+?(?=[0-9])";
		
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);

		// Create matcher object.
		Matcher m = r.matcher(result);
		if (m.find( )) {
          logger.info("Found value1: " + m.group() );
          DOI = m.group();
          
          //cleaning openAIRE's DOIs which have stripped from dashes
          if(DOI.contains(" ")){
           DOI= DOI.replaceAll(" ","-");
           //logger.info("DOI is: " +DOI );
           
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

return triplesRDD_1.union(triplesRDD_2);*/