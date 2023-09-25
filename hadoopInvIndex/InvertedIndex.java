import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private final Text word = new Text();
        private final Text docId = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Obtén el nombre del archivo actual
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();            
            docId.set(fileName);
            
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, docId);
            }
        }
    }
    
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            
            for (Text value : values) {
                sb.append(value.toString()).append(",");
            }
            
            context.write(key, new Text(sb.toString()));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        
        // Configura las clases del Mapper y Reducer
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        
        // Configura los tipos de salida del Mapper y Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Configura las rutas de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Finaliza el trabajo y espera a que se complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

