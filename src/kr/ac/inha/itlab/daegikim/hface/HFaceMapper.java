package kr.ac.inha.itlab.daegikim.hface;

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

public class HFaceMapper extends Mapper<Object, Text, Text, Text> {
    public static CascadeClassifier faceDetector = null;
    public static MatOfRect faceDetections = null;

    /**
     * Test map method for Detecting face area.
     * @param key HDFS 파일 경로
     * @param value 바이트 배열로 기록된 이미지 데이터
     * @param context 하둡 컨텍스트 객체
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        System.loadLibrary("opencv_java245");
        faceDetector = new CascadeClassifier("lbpcascade_frontalface.xml");
        faceDetections = new MatOfRect();

        Rect rect = new Rect();

        try {
            rect = faceDetection(value.toString());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Text rectText = new Text(rect.toString());
        context.write(rectText, value);
    }

    /**
     * 파일 이름을 전달받아 얼굴 영억을 검출한 후 해당 위치를 반환한다.
     * @param filename 검출할 이미지 파일명
     * @return 얼굴 영역
     * @throws NoSuchAlgorithmException
     */
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
