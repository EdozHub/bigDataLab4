package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		int n = 0;
        double sum = 0, avg = 0;
        for (DoubleWritable value : values){
            sum += value.get();
            n += 1;
        }
    	avg = sum / n;
        context.write(key, new Text(avg + ""));
    }
}
