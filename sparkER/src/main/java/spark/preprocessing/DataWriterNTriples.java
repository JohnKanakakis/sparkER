package spark.preprocessing;

import java.net.URLEncoder;
import java.util.Set;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class DataWriterNTriples {

	
	public static void saveTriples(JavaPairRDD<String, Tuple2<String, String>> triplesRDD,String outputDirectory){
		
		triplesRDD.map(new Function<Tuple2<String, Tuple2<String, String>>, Text>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Text call(Tuple2<String, Tuple2<String, String>> triple)
					throws Exception {
				Text text = new Text();
				
				String subject = triple._1;
				String predicate = triple._2._1;
				String object = triple._2._2;
				
				text.set(subject +" "+predicate+" "+object);
				return text;
			}
		}).saveAsTextFile(outputDirectory);
		
	}

	public static void saveEntities(JavaPairRDD<String, Set<Tuple2<String, String>>> entitiesRDD,
						    String outputDirectory) {
		
		entitiesRDD
		.map(new Function<Tuple2<String, Set<Tuple2<String, String>>>, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Set<Tuple2<String, String>>> entity)
					throws Exception {
				//Text text = new Text();
				
				String subject = entity._1;
				String triples = "";
				for(Tuple2<String, String> po : entity._2){
					String predicate = po._1;
					String object = po._2;
					
					if(UrlValidator.getInstance().isValid(object)){
						object = "<"+object+">";
					}
					
					triples+=("<"+subject+">" +" "+"<"+predicate+">"+" "+object+"\n");
				}
				//text.set(triples);
				return triples;
			}
		}).saveAsTextFile(outputDirectory);
		
	}
}
