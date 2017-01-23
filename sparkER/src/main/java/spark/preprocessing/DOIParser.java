package spark.preprocessing;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DOIParser {

	
	public static String parseDOI(String line){
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
//		       String pattern="10.\\d{4}/\\d{3}/.*";
			//String pattern="10.\\d{4}/.*";
			String pattern = "10.\\d{4}/.+(=?).+?(?=[0-9])";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);

			// Create matcher object.
			Matcher m = r.matcher(result);
			if (m.find( )) {
	          //System.out.println("Found value1: " + m.group() );
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
}
