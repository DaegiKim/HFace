package kr.ac.inha.itlab.daegikim.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ImageDuplicatesRemover {

    public static class ImageMd5Mapper extends Mapper<Text, BytesWritable, Text, Text> {
        public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {
            //get the md5 for this specific file
            String md5Str;
            try {
                md5Str = calculateMd5(value.getBytes());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                context.setStatus("Internal error - can't find the algorithm for calculating the md5");
                return;
            }
            Text md5Text = new Text(md5Str);

            //put the file in the map where the md5 is the key, so duplicates will
            // be grouped together for the reduce function
            context.write(md5Text, key);
        }


        static String calculateMd5(byte[] imageData) throws NoSuchAlgorithmException {
            //get the md5 for this specific data
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(imageData);
            byte[] hash = md.digest();

            // Below code of converting Byte Array to hex
            String hexString = new String();
            for (int i=0; i < hash.length; i++) {
                hexString += Integer.toString( ( hash[i] & 0xff ) + 0x100, 16).substring( 1 );
            }
            return hexString;
        }

    }

    public static class ImageDupsReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //Key here is the md5 hash while the values are all the image files that
            // are associated with it. for each md5 value we need to take only
            // one file (the first)
            Text imageFilePath = null;
            for (Text filePath : values) {
                imageFilePath = filePath;
                break;//only the first one
            }
            // In the result file the key will be again the image file path.
            context.write(imageFilePath, key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //This is the line that makes the hadoop run locally
        //conf.set("mapred.job.tracker", "local");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ImageDuplicatesRemover <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "image dups remover");
        job.setJarByClass(ImageDuplicatesRemover.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ImageMd5Mapper.class);
        job.setReducerClass(ImageDupsReducer.class);
        //job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}