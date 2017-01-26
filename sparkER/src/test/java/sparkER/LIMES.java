package sparkER;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.aksw.limes.core.execution.planning.plan.Instruction;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;

import spark.HDFSUtils;

public class LIMES {

	public static void main(String args[]) throws FileNotFoundException{
		
		InputStream configFile = new FileInputStream("src/test/resources/config.xml");
		InputStream dtdFile = new FileInputStream("src/test/resources/limes.dtd");

		XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = reader.validateAndRead(configFile,dtdFile);

		System.out.println(config.getPrefixes());
		
		System.out.println(config.getMetricExpression());
		System.out.println(config.getSourceInfo().getProperties());
		System.out.println(config.getTargetInfo().getProperties());
		
		
		
		
		Rewriter rw = RewriterFactory.getRewriter("Default");
		LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
		LinkSpecification rwLs = rw.rewrite(ls);
		IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), null, null);
		assert planner != null;
		NestedPlan plan = planner.plan(rwLs);
		System.out.println(plan.getAllMeasures());
		
		for(Instruction i : plan.getInstructionList()){
			System.out.println("exp:"+i.getMeasureExpression()+"\t"+i.getThreshold());
		}
		
		System.out.println(plan.getSubPlans());
		System.out.println(plan.getOperator());
	}
}
