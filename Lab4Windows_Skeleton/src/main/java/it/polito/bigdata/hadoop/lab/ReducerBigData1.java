package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {   
        
        
        double avg = 0;
        int sum = 0;
        int n = 0;
        HashMap<String, Double> reviews = new HashMap<>();    
        for (Text value : values){
            String vals[] = value.toString().split(" ");
            sum += Integer.parseInt(vals[1]);
            n += 1;
            reviews.put(vals[0], Double.parseDouble(vals[1]));
        }
        avg = sum / n;
        double normalizedPrice;
        for (Entry<String, Double> val : reviews.entrySet()){
            normalizedPrice = val.getValue() - avg;
            String k = val.getKey();
            context.write(new Text(k), new DoubleWritable(normalizedPrice));
        }
    }
}
