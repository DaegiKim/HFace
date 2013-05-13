package kr.ac.inha.itlab.daegikim.hface;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Rect;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class HFaceMapper extends Mapper<Text, BytesWritable, Text, Text> {
    public static CascadeClassifier faceDetector = null;
    public static MatOfRect faceDetections = null;

    public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {
        System.loadLibrary("opencv_java245");
        faceDetector = new CascadeClassifier("lbpcascade_frontalface.xml");
        faceDetections = new MatOfRect();

        Rect rect = new Rect();

        String str = key.toString();
        int s = str.lastIndexOf("/");
        String sub = str.substring(s,str.length());

        try {
            rect = faceDetection(sub);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Text rectText = new Text(rect.toString());
        context.write(rectText, key);
    }

    public static Rect faceDetection(String filename) throws NoSuchAlgorithmException {
        Mat image = Highgui.imread("image/"+filename);
        faceDetector.detectMultiScale(image, faceDetections, 1.4, 1, 0, new Size(128, 128), new Size(256, 256));

        for (Rect rect : faceDetections.toArray()) {
            //Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0), 2);
            return rect;
        }
        return new Rect();
    }
}
