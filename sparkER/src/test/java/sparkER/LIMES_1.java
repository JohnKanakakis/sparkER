package sparkER;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.aksw.limes.core.execution.engine.ExecutionEngine;
import org.aksw.limes.core.execution.engine.ExecutionEngineFactory;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.plan.Plan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.cache.HybridCache;
import org.aksw.limes.core.io.cache.Instance;
import org.aksw.limes.core.io.cache.MemoryCache;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.aksw.limes.core.io.mapping.Mapping;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.aksw.limes.core.io.serializer.ISerializer;
import org.aksw.limes.core.io.serializer.SerializerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.ParserAdapter;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;

import scala.Tuple2;
import spark.DatasetManager;
import spark.SparkUtils;


public class LIMES_1 {

	public static void main(String[] args) throws IOException, SAXException {
		// TODO Auto-generated method stub
		/*
		String s = "Using just-in-time information to support scientific discovery learning in a computer-based simulation.";
		System.out.println(s.replaceAll("[^A-Za-z0-9]", " "));
		System.exit(0);*/
		/*System.out.println(RDF.type);
		String resource = "dfdfdfdfdfdf_d1";
		int pos = "dfdfdfdfdfdf_d1".lastIndexOf("_d")+"_d".length();
		int datasetId = Integer.parseInt(resource.substring(pos));
		System.out.println(datasetId);
		System.exit(0);*/
		
	/*	String literal = "\"Fouc\"^^\"aut\"^^<http://www.w3.org/2001/XMLSchema-datatypesstring>";
		System.out.println(DataFormatter.eliminateDataTypeFromLiteral(literal));
		System.exit(0);*/
		
		XMLConfigurationReader reader = new XMLConfigurationReader();
    	
		org.aksw.limes.core.io.config.Configuration config = null;
		
		InputStream configFile = FileUtils.openInputStream(new File("src/test/resources/configDemo.xml"));
		InputStream dtdFile = FileUtils.openInputStream(new File("src/test/resources/limes.dtd"));;
		config = reader.validateAndRead(configFile,dtdFile);
		
		System.out.println(config.getSourceInfo().getProperties());
		System.out.println(config.getTargetInfo().getProperties());
		
		
		byte[] skbBinary = SparkUtils.serialize(config.getSourceInfo());
        byte[] tkbBinary = SparkUtils.serialize(config.getTargetInfo());
        
		String s1 = "";
		String s2 = "";
		try {
			s1 = FileUtils.readFileToString(new File("src/test/resources/source_demo.nt"));
			s2 = FileUtils.readFileToString(new File("src/test/resources/source_demo1.nt"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MemoryCache m1 = readTTL(s1,(KBInfo) SparkUtils.deserialize(skbBinary),config.getPrefixes());
		MemoryCache m2 = readTTL(s2,(KBInfo) SparkUtils.deserialize(tkbBinary),config.getPrefixes());
		
		
		
		
		
		System.out.println("source cache "+m1.size());
		System.out.println("target cache "+m2.size());
		
		System.out.println("start execution");
		long t1 = System.currentTimeMillis();
		
		//System.out.println("config is "+config.toString());
        Rewriter rw = RewriterFactory.getRewriter("Default");
        LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
        LinkSpecification rwLs = rw.rewrite(ls);
        // 4.3. Planning
       // System.out.println("planning");
        IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), m1, m2);
        assert planner != null;
        NestedPlan plan = planner.plan(rwLs);
       // System.out.println("plan is "+plan);
        
        //List<NestedPlan> subPlans = plan.getSubPlans();
        //byte[] subPlansBinary = Utils.serialize(subPlans);
        
        byte[] planBinary = SparkUtils.serialize(plan);
        plan = (NestedPlan) SparkUtils.deserialize(planBinary);
        
       // subPlans = (List<NestedPlan>) Utils.deserialize(subPlansBinary);
        //plan.setSubPlans(subPlans);
        
       // System.out.println("dplan = "+plan);
       // System.out.println("ls is "+rwLs.toString());
       
        //System.out.println(plan.getOperator());
        // 5. Execution
        ExecutionEngine engine = ExecutionEngineFactory.getEngine("Default", m1, m2,
                config.getSourceInfo().getVar(), config.getTargetInfo().getVar());
        assert engine != null;
        
        
        Mapping verificationMapping = engine.execute(plan);
        
        
        Mapping acceptanceMapping = verificationMapping.getSubMap(config.getAcceptanceThreshold());
        
        long t2 = System.currentTimeMillis();
        System.out.println("execution finished. It took "+ (t2-t1)/1000+" secs");
        
       // System.out.println("thres "+config.getAcceptanceThreshold());
        
        
        
        HashMap<String, Double> targets;
        HashMap<String,String> results = new HashMap<String,String>();
        double thres;
        int cnt = 0;
        
        
        for(String source: verificationMapping.getMap().keySet()){
        	//System.out.println("source:"+source);
        
        	
        	targets = verificationMapping.getMap().get(source);
        	/*if(targets.size() > 1){
        		System.out.println("more than 1 !!!!!!!!!!!!");
        	}*/
        	if(targets.size() == 0){
        		System.out.println("no targets for source "+source);
        		System.exit(0);
        	}
        	double maxThres = 0.0;
        	String maxTarget = "";
        	int loopCnt = 0;
        	for(String target: targets.keySet()){
        		if(loopCnt == 0){
        			maxTarget = target;
        			maxThres = targets.get(target).doubleValue();
        			loopCnt++;
        		}
        		thres = targets.get(target).doubleValue();
        		 
        		if(thres >= maxThres){
        			maxThres = thres;
        			maxTarget = target;
        		}
        	}
        	if(maxThres >= config.getAcceptanceThreshold()){
        		//System.out.println(source +"->"+maxTarget +"|"+maxThres);
    			cnt++;
    			
    			results.put(source, maxTarget+"|"+maxThres);
        	}
        	
        }
        System.out.println("links = "+cnt);
        System.out.println("result size "+ results.size());
       /* for(String source:results.keySet()){
        	if(results.get(source).size() > 1){
        		System.out.println(source +" "+results.get(source));
        	}
        }*/
        System.out.println("verifications :"+verificationMapping.getMap().size());
       
        
        System.out.println("accepted mappings!"+acceptanceMapping);
        
        for(String source: acceptanceMapping.getMap().keySet()){
        	if(acceptanceMapping.getMap().get(source).size() > 1){
        		System.out.println("source "+source+" has "+acceptanceMapping.getMap().get(source).size()+" mappings");
        	}
        }
        
        System.exit(0);
        
        RDFParser p = Rio.createParser(RDFFormat.NTRIPLES);
		
		ArrayList<Statement> myList = new ArrayList<Statement>();
		StatementCollector collector = new StatementCollector(myList);
		p.setRDFHandler(collector);
		p.parse(new FileInputStream(new File("links.nt")),"");
		HashMap<String,String> realLinks = new HashMap<String,String>();
		for(int i = 0 ; i < myList.size(); i++){
			Statement st = myList.get(i);
			String subject = st.getSubject().toString();
			//String predicate = st.getPredicate().toString();
			String object = st.getObject().toString();
			realLinks.put(subject, object);
		}
		System.out.println("real links = "+realLinks.size());
		
		
		int correctLinks = 0;
		int wrongLinks = 0;
		int missedLinks = 0;
		for(String source : realLinks.keySet()){
			if(results.containsKey(source)){
				String target = results.get(source);
				target = target.substring(0, target.indexOf("|"));
				if(target.equals(realLinks.get(source))){
					//correct link
					correctLinks++;
				}else{
					//correct source not correct target
					System.out.println("source:"+source);
					System.out.println("target:"+results.get(source));
					System.out.println("correct target:"+realLinks.get(source));
					wrongLinks++;
				}
			}else{
				//missed link
				System.out.println("missed source:"+source);
				System.out.println(m1.getInstance(source).toString());
				System.out.println(verificationMapping.getMap().get(source));
				missedLinks++;
			}
		}
		System.out.println("correct links "+correctLinks);
		System.out.println("wrong links "+wrongLinks);
		System.out.println("missed links "+missedLinks);
		
		//System.out.println(acceptanceMapping);
		//System.out.println("specific target "+m2.getInstance("http://dblp.l3s.de/d2r/resource/publications/journals/ile/HulshofJ06").toString());
		
	}
	
	private static MemoryCache readTTL(String s, KBInfo kb, HashMap<String, String> prefixes){
		MemoryCache m = new MemoryCache();
		
		try {
			//in = new FileInputStream("input.ttl");
			RDFParser p = Rio.createParser(RDFFormat.NTRIPLES);
			
			ArrayList<Statement> myList = new ArrayList<Statement>();
			StatementCollector collector = new StatementCollector(myList);
			p.setRDFHandler(collector);
			
			
			InputStream in = new ByteArrayInputStream(s.getBytes());
			
			p.parse(in,"");
			
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
			Statement st = null;
			
			HashMap<String,List<String>> resources = new HashMap<String,List<String>>();
			String subject;
			String predicate;
			String object;
			List<String> POs;
			for(int i = 0 ; i < myList.size(); i++){
				st = myList.get(i);
				subject = st.getSubject().toString();
				predicate = st.getPredicate().toString();
				
				object = st.getObject().toString();
				if(resources.containsKey(subject)){
					POs = resources.get(subject);
				}else{
					POs = new ArrayList<String>();
				}
				POs.add(predicate);
				POs.add(object);
				resources.put(subject, POs);
			}
			
			ArrayList<String> flatResources = new ArrayList<String>();
			for(Entry<String,List<String>> entry : resources.entrySet()){
				flatResources.add("@@@");
				flatResources.add(entry.getKey());
				for(String str : entry.getValue()){
					flatResources.add(str);
				}
			}
			//for(int i = 0; i < flatResources.size();i++)
				//System.out.println(flatResources.get(i));
			
			//System.out.println(resources);
			for(int i = 0; i < flatResources.size()-2; i++){
				if(flatResources.get(i).equals("@@@")){
					//cnt++;
					subject = flatResources.get(i+1);
				  	//datasetId = DatasetManager.getDatasetIdOfResource(subject);
				  	
				  	int j = i+2;
				  	while(j < flatResources.size() && !flatResources.get(j).equals("@@@")  ){
				  		predicate = flatResources.get(j);
				  		
				  		
				  		predicate = shrinkURI(predicate,prefixes);
				  		
				  		
				  		if(kb.getProperties().contains(predicate)){
				  			object = SparkUtils.eliminateDataTypeFromLiteral(flatResources.get(j+1));
				  			if(predicate.equals("rdf:type")){
					  			object = shrinkURI(object,prefixes);
					  		}
				  			
				    		if(kb.getFunctions().get(predicate).keySet().size() == 0){
								m.addTriple(subject, predicate, object);
				    		}
							else{
								for(String propertyDub : kb.getFunctions().get(predicate).keySet()) {
									//System.out.println("property dub ->"+propertyDub);
									String value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
									
									m.addTriple(subject, propertyDub, value);
								}
							}	
						}	
				  		j = j + 2;
				  	}
				}
			}
			//System.out.println(m);
			//System.exit(0);
			//System.out.println(kb.getProperties());
			HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
			//System.out.println("properties = "+configProperties);
			
			for(String sub : resources.keySet()){
				
				POs = resources.get(sub);
				String record = "";
				for(int i = 0; i < POs.size()-1; i = i+2){
					predicate = POs.get(i);
					object = POs.get(i+1);
				
					if(kb.getProperties().contains(predicate)){
						//System.out.println("examing property: "+st.getPredicate().toString());
						
						object = SparkUtils.eliminateDataTypeFromLiteral(object);
						//remove localization information, e.g. @en
						record+=(object.replace("\"", "")+" ");
						if(kb.getFunctions().get(predicate).keySet().size() == 0){
							m.addTriple(sub, predicate, object);
							
						}else{
							
							for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
								
								//System.out.println("propertyDub is "+propertyDub);
								
								String value = Preprocessor.process(object, kb.getFunctions()
																			  .get(predicate)
																			  .get(propertyDub));
								
								//System.out.println("processed value "+value);
								
								m.addTriple(sub, propertyDub, value);
								
							}
						}
						
						
					}
					
					
					else{
						//System.out.println("adding statement "+ st.toString());
						m.addTriple(sub, predicate, object);
					}
				}
				//System.out.println("record to index = "+record);
				String[] tokens = record.split(" ");
				for(int i = 0; i < tokens.length; i++){
					if(tokens[i].length() > 3){
						tokens[i] = tokens[i].trim();
						//System.out.println("token pair ("+ tokens[i]+","+sub+")");
						//t = new Tuple2<String,String>(tokens[i],r_id);
						//tokenPairs.add(t);
						//tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
					}
				}
				//System.out.println("record to index = "+record);
			}
		
		} catch (IOException | RDFParseException | RDFHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//System.out.println(m);
		return m;
	}

	private static String shrinkURI(String predicate,
			HashMap<String, String> prefixes) {
		
		//invert prefixes
		HashMap<String, String> invertedPrefixes = new HashMap<String, String>();
		
		for(Entry<String,String> entry: prefixes.entrySet()){
			invertedPrefixes.put(entry.getValue(), entry.getKey());
		}
		//System.out.println(invertedPrefixes);
		
		
		
		String baseURI;
		String propertyName;
		int indexOfBaseURIDelimeter;
		if(predicate.contains("#")){
			indexOfBaseURIDelimeter = predicate.lastIndexOf("#");
		}else{
			indexOfBaseURIDelimeter = predicate.lastIndexOf("/");
		}
		
		baseURI = predicate.substring(0,indexOfBaseURIDelimeter+1);
		propertyName = predicate.substring(indexOfBaseURIDelimeter+1);
		//System.out.println(predicate+" ---> "+baseURI + " + "+propertyName);
		
		String prefix = invertedPrefixes.get(baseURI);
		
		if(prefix == null){
			System.out.println("invalid baseURI "+baseURI);
			System.exit(0);
		}
		
		return prefix+":"+propertyName;
	}

}
