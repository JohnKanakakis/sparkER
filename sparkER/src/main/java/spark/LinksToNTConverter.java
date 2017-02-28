package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class LinksToNTConverter {

	public static final String sameAsURI = "http://www.w3.org/2002/07/owl#sameAs";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String inputLinksSERFile = args[0];
		String outputNTFile = args[1];
		
		
		SparkConf sparkConf = new SparkConf().setAppName("LinksToNTConverter");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		
		JavaRDD<Tuple2<String, String>> links = ctx.objectFile(inputLinksSERFile);
		
		Function<Tuple2<String, String>, String> convertToNT = 
				new Function<Tuple2<String, String>, String>(){

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> linkPair)
							throws Exception {
						String sourceURI = linkPair._1;
						String targetURI = linkPair._2;
						
						return 	"<"+sourceURI+">" + "\t" + 
								"<"+sameAsURI+">" + "\t" +
								"<"+targetURI+">" +	".\n";
					}
			
		};
		
		links.map(convertToNT).saveAsTextFile(outputNTFile);;
		
		
		ctx.close();
	}

}
