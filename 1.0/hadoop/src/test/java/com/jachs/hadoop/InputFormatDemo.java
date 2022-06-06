package com.jachs.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.join.ComposableInputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/***
 * 
 * @author zhanchaohan
 *
 */
public class InputFormatDemo {
//    BaileyBorweinPlouffe.BapInputFormat ;
	ComposableInputFormat<WritableComparable, Writable>composableInputFormat;
	CompositeInputFormat<WritableComparable>compositeInputFormat;
	DBInputFormat<DBWritable>dBInputFormat;
//	DistSum.Machine.Abst
	FileInputFormat<String, Integer>fileInputFormat;
	
	//FileInputFormat
	CombineFileInputFormat<String, Integer>combineFileInputFormat;
	KeyValueTextInputFormat keyValueTextInputFormat;
	NLineInputFormat nlineInputFormat;
	SequenceFileInputFormat<String, Integer>sequenceFileInputFormat;
	TextInputFormat textInputFormat;
}
