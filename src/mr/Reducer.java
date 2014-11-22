/**
 * 
 */
package mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.PriorityQueue;

import mr.io.TextWritable;
import mr.io.Writable;

/**
 * @author Nicolas_Yu
 * 
 */
public abstract class Reducer<K1, V1, K2, V2> implements Serializable {

	private static final long serialVersionUID = 1L;

	private String shuffleDir;
	private PriorityQueue<RecordLine> records;
	private ArrayList<RecordLine> reduceLines;
	private String id;

	public abstract void reduce(TextWritable key, Iterable<Writable> values,
			Context context);

	/**
	 * Initialize the reducer 
	 * @param shuffleDir
	 */
	public void initialize(String shuffleDir) {
		this.shuffleDir = shuffleDir;
		records = new PriorityQueue<RecordLine>();
		reduceLines = new ArrayList<RecordLine>();
	}

	/**
	 * Merge partitions from different mapper hosts
	 */
	public void mergePartition() {
		try {
			System.out.println("ShuffleDir is " + this.shuffleDir);
			File dir = new File(shuffleDir);
			File[] files = dir.listFiles();
			for (File file : files) {
				
				if(!file.getName().endsWith(id)){
					continue;
				}
				
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line = null;

				while ((line = br.readLine()) != null) {
					String[] splits = line.split("\t");
					if (splits.length < 2) {
						continue;
					}
					TextWritable key = new TextWritable();
					TextWritable value = new TextWritable();
					key.setVal(splits[0]);
					value.setVal(splits[1]);
					RecordLine record = new RecordLine(key);
					record.addValue(value);
					records.add(record);
				}
				br.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void combineValue() {
		System.out.println("Records Size:" + records.size());
		if (records.isEmpty()) {
			return;
		}
		RecordLine item = records.poll();
		Writable currentKey = item.getKey();
		while (!records.isEmpty()) {
			Writable key = records.peek().getKey();
			if (key.getVal().hashCode() == currentKey.getVal().hashCode()) {
				RecordLine temp = records.poll();
				item.addValue((Writable) temp.getValue().iterator().next());
			} else {
				reduceLines.add(item);
				item = records.poll();
				currentKey = item.getKey();
			}
		}
	}

	/**
	 * @return the shuffleDir
	 */
	public String getShuffleDir() {
		return shuffleDir;
	}

	/**
	 * @param shuffleDir
	 *            the shuffleDir to set
	 */
	public void setShuffleDir(String shuffleDir) {
		this.shuffleDir = shuffleDir;
	}

	/**
	 * @return the records
	 */
	public PriorityQueue<RecordLine> getRecords() {
		return records;
	}

	/**
	 * @param records
	 *            the records to set
	 */
	public void setRecords(PriorityQueue<RecordLine> records) {
		this.records = records;
	}

	/**
	 * @return the reduceLines
	 */
	public ArrayList<RecordLine> getReduceLines() {
		return reduceLines;
	}

	/**
	 * @param reduceLines
	 *            the reduceLines to set
	 */
	public void setReduceLines(ArrayList<RecordLine> reduceLines) {
		this.reduceLines = reduceLines;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
