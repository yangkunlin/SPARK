package utils;

import java.io.*;

/**
 * <P>I/O操作辅助工具</P>
 *
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public final class IoUtil {

    /**
     * <P>关闭输出流</P>
     *
     * @param os 输出流
     */
    public static void closeOutputStream(OutputStream os) {
        if (os == null) {
            return;
        }
        try {
            os.close();
        } catch (IOException ioe) {
        }
    }

    /**
     * <P>关闭输入流</P>
     *
     * @param is 输入流
     */
    public static void closeInputStream(InputStream is) {
        if (is == null) {
            return;
        }
        try {
            is.close();
        } catch (IOException ioe) {
        }
    }

    /**
     * <P>关闭 {@link Writer}</P>
     *
     * @param writer {@link Writer}
     */
    public static void closeWriter(Writer writer) {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException ioe) {
        }
    }

    /**
     * <P>关闭 {@link Reader}</P>
     *
     * @param reader {@link Reader}
     */
    public static void closeReader(Reader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (IOException e) {
        }
    }
}
