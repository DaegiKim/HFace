package kr.ac.inha.itlab.daegikim.hface;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HFaceReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text imageFilePath = null;
        for (Text filePath : values) {
            imageFilePath = filePath;
            break;
        }
        context.write(imageFilePath, key);
    }
}