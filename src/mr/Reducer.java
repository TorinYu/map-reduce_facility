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
public class Reducer <K1, V1, K2, V2> implements Serializable{

	private static final long serialVersionUID = 1L;

	private String shuffleDir;
	private PriorityQueue<RecordLine> records;
	private ArrayList<RecordLine> reduceLines;


	public void reduce(TextWritable key, Iterable<Writable> values, Context context) {

	}

	public void initialize(String shuffleDir) {
		this.shuffleDir = shuffleDir;
		records = new PriorityQueue<RecordLine>();
		reduceLines = new ArrayList<RecordLine>();
	}

	public void mergePartition() {
		File dir = new File(shuffleDir);
		File[] files = dir.listFiles();
		for (File file : files) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line = null;

				while ((line = br.readLine()) != null) {
					String[] splits = line.split("\t");
					TextWritable key = new TextWritable();
					TextWritable value = new TextWritable();
					key.setVal(splits[0]);
					value.setVal(splits[1]);
					RecordLine record = new RecordLine(key);
					record.addValue(value);
					records.add(record);
				}
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void combineValue() {
		if (records.isEmpty()) {
			return;
		}
		RecordLine item = records.poll();
		Writable currentKey = item.getKey();
		while (!records.isEmpty()) {
			Writable key = records.peek().getKey();
			if (key.getVal().hashCode() == currentKey.getVal().hashCode()) {
				RecordLine temp = records.poll();
				item.addValue((Writable)temp.getValue().iterator().next());
			} else {
				reduceLines.add(item);
				item = records.poll();
				currentKey = item.getKey();
			}
		}
	}

}
