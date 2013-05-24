package kr.ac.inha.itlab.daegikim.hface;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HFaceReducer extends Reducer<Text,Text,Text,Text> {

    /**
     * Test reduce method for write the result by text.
     * @param key 얼굴 영역 (x,y 좌표, 가로 길이, 세로 길이)
     * @param values HDFS 파일 경로
     * @param context 하둡 컨텍스트
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text imageFilePath = null;
        for (Text filePath : values) {
            imageFilePath = filePath;
            break;
        }
        context.write(imageFilePath, key);
    }
}