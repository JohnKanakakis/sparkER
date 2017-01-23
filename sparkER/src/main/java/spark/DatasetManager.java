package spark;

import java.io.Serializable;

/**
 * The DatasetManager is used to add a data set stamp to each entity URI
 * so as to distinguish between source and target URIs.
 * 
 * @author John Kanakakis
 *
 */
public class DatasetManager implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final String DATASET_STAMP = "_";
	private static DatasetManager dm = null;
	
	public static DatasetManager get() {
		// TODO Auto-generated method stub
		if(dm == null){
			dm = new DatasetManager();
		}
		return dm;
	}
	
	
	
	public static String addDatasetIdToEntity(String entity, String datasetId){
		
		return entity+DATASET_STAMP+datasetId;
	}

	public static String getDatasetIdOfEntity(String entity){
		
		int pos = entity.lastIndexOf(DATASET_STAMP)+DATASET_STAMP.length();
		String datasetId = entity.substring(pos);
		return datasetId;
	}

	public static String removeDatasetIdFromEntity(String entity) {
		// TODO Auto-generated method stub
		int pos = entity.lastIndexOf(DATASET_STAMP);
		
		return entity.substring(0,pos);
	}
}
