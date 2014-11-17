/**
 * 
 */
package mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

import mr.Type.TASK_TYPE;
import mr.io.TextWritable;
import mr.io.Writable;

/**
 * @author Nicolas_Yu
 * 
 */
public class Context {

	private String jobId = null;
	private String taskId = null;
	private int reduceNum = 0;
	private TASK_TYPE taskType = null;
	private int numOfFiles = 0;
	private String mapContentFilePath = "";

	private String partitionOutPath = "";
	private int bufferSize = 0;

	private ArrayList<RecordLine> mapContent;

	/**
	 * Constructor
	 * 
	 * @param jobId
	 * @param taskId
	 * @param reduceNum
	 * @param partitionOutPath
	 * @param type
	 */
	public Context(String jobId, String taskId, int reduceNum,
			String partitionOutPath, TASK_TYPE type) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.reduceNum = reduceNum;
		this.taskType = type;
		this.partitionOutPath = partitionOutPath;
		this.mapContentFilePath = "/tmp/" + taskId + "/tmp/";
		this.bufferSize = 900;
		this.mapContent = new ArrayList<RecordLine>(this.bufferSize);
	}

	/*
	 * Combined small map partitions to (reduceNum)Map outputs for Reducer
	 */
	public void partionMapContent() throws IOException {

		File writeOutPath = new File(partitionOutPath);
		if (!writeOutPath.exists()) {
			boolean made = writeOutPath.mkdirs();
			// System.out.println("Did we make it? " + made);
			// System.out.println("Output Path is " + writeOutPath);
		}
		if (!mapContent.isEmpty()) {
			writeToFile();
		}
		HashMap<Integer, BufferedReader> mapBufferFiles = new HashMap<Integer, BufferedReader>();

		for (int i = 0; i < numOfFiles; i++) {
			mapBufferFiles.put(i, new BufferedReader(new FileReader(
					mapContentFilePath + i)));
		}

		PriorityQueue<RecordLine> records = new PriorityQueue<RecordLine>();
		for (int i = 0; i < numOfFiles; i++) {
			String line;
			while ((line = mapBufferFiles.get(i).readLine()) != null) {
				String[] words = line.split("\t");
				TextWritable key = new TextWritable();
				TextWritable value = new TextWritable();
				key.setVal(words[0]);
				value.setVal(words[1]);
				RecordLine record = new RecordLine(key);
				record.addValue(value);
				records.add(record);
			}
		}

		HashMap<Integer, BufferedWriter> partitionFiles = new HashMap<Integer, BufferedWriter>();
		for (int i = 0; i < reduceNum; i++) {
			partitionFiles.put(i, new BufferedWriter(new FileWriter(
					partitionOutPath + taskId + "#" + i)));
		}

		while (!records.isEmpty()) {
			RecordLine record = records.poll();
			String key = (String) record.getKey().getVal();
			String value = (String) record.getValue().iterator().next()
					.getVal();
			int partitionId = Math.abs(key.hashCode() % reduceNum);
			String line = key + "\t" + value + "\n";
			// System.out.println("line to be written is " + line);
			partitionFiles.get(partitionId).write(line);
		}

		for (int i = 0; i < reduceNum; i++) {
			partitionFiles.get(i).close();
		}
		for (int i = 0; i < numOfFiles; i++) {
			mapBufferFiles.get(i).close();
		}

	}

	/**
	 * write the content to file when buffer is full for Map
	 */
	private void writeToFile() {
		// System.out.println("Entering Write to File");

		File pathFile = new File(mapContentFilePath);
		if (!pathFile.exists()) {
			pathFile.mkdirs();
		}

		try {
			File file = new File(mapContentFilePath + numOfFiles);
			if (taskType == TASK_TYPE.Mapper) {
				// System.out.println(taskType.toString());
				numOfFiles++;
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			Collections.sort(this.mapContent);
			for (RecordLine record : mapContent) {
				// System.out.println("To Write: " + record.getKey().getVal() );
				bw.write(record.getKey().getVal() + "\t"
						+ record.getValue().iterator().next().getVal());
				bw.write("\n");
			}
			bw.close();
			mapContent.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write a record to Context
	 * 
	 * @param key
	 * @param value
	 */
	public void write(Writable key, Writable value) {
		// System.out.println("Write a new record " + key.getVal() + " " +
		// value.getVal());
		RecordLine record = new RecordLine(new TextWritable(key.getVal()));
		record.addValue(value);
		this.mapContent.add(record);

		if (mapContent.size() == bufferSize) {
			writeToFile();
		}
	}

	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @param jobId
	 *            the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId
	 *            the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the reduceNum
	 */
	public int getReduceNum() {
		return reduceNum;
	}

	/**
	 * @param reduceNum
	 *            the reduceNum to set
	 */
	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}

	/**
	 * @return the taskType
	 */
	public TASK_TYPE getTaskType() {
		return taskType;
	}

	/**
	 * @param taskType
	 *            the taskType to set
	 */
	public void setTaskType(TASK_TYPE taskType) {
		this.taskType = taskType;
	}

	/**
	 * @return the numOfFiles
	 */
	public int getNumOfFiles() {
		return numOfFiles;
	}

	/**
	 * @param numOfFiles
	 *            the numOfFiles to set
	 */
	public void setNumOfFiles(int numOfFiles) {
		this.numOfFiles = numOfFiles;
	}

	/**
	 * @return the mapContentFilePath
	 */
	public String getMapContentFilePath() {
		return mapContentFilePath;
	}

	/**
	 * @param mapContentFilePath
	 *            the mapContentFilePath to set
	 */
	public void setMapContentFilePath(String mapContentFilePath) {
		this.mapContentFilePath = mapContentFilePath;
	}

	/**
	 * @return the partitionOutPath
	 */
	public String getPartitionOutPath() {
		return partitionOutPath;
	}

	/**
	 * @param partitionOutPath
	 *            the partitionOutPath to set
	 */
	public void setPartitionOutPath(String partitionOutPath) {
		this.partitionOutPath = partitionOutPath;
	}

	/**
	 * @return the bufferSize
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	/**
	 * @param bufferSize
	 *            the bufferSize to set
	 */
	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	/**
	 * @return the mapContent
	 */
	public ArrayList<RecordLine> getMapContent() {
		return mapContent;
	}

	/**
	 * @param mapContent
	 *            the mapContent to set
	 */
	public void setMapContent(ArrayList<RecordLine> mapContent) {
		this.mapContent = mapContent;
	}
}
