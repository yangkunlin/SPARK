package utils.ikanalyzer;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author YKL on 2018/4/9.
 * @version 1.0
 *          说明：
 *          XXX
 */
public class WordSplit {

    //停用词词表
    public static final String stopWordTable = "src/stopword.dic";

    public static String wordSplit(String word) throws IOException {

        //读入停用词文件
        BufferedReader StopWordFileBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(stopWordTable))));
        //用来存放停用词的集合
        Set<String> stopWordSet = new HashSet<String>();
        //初如化停用词集
        String stopWord = null;
        for (; (stopWord = StopWordFileBr.readLine()) != null; ) {
            stopWordSet.add(stopWord);
        }
//        //测试文本
//        String text = "不同于计算机，人类一睁眼就能迅速看到和看明白一个场景，因为人的大脑皮层至少有一半以上海量神经元参与了视觉任务的完成。";
        //创建分词对象
        StringReader sr = new StringReader(word);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        String splitedWord = "";
        //分词
        while ((lex = ik.next()) != null) {
            //去除停用词
            if (stopWordSet.contains(lex.getLexemeText())) {
                continue;
            }
            splitedWord += lex.getLexemeText().trim() + "\t";
        }
        //关闭流
        StopWordFileBr.close();

        return splitedWord;
    }

//    public static void main(String[] args) {
//        String text = "不同于计算机，人类一睁眼就能迅速看到和看明白一个场景，因为人的大脑皮层至少有一半以上海量神经元参与了视觉任务的完成。";
//        String splitedWord = "";
//        try {
//            splitedWord = wordSplit(text);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println(splitedWord);
//    }

}
