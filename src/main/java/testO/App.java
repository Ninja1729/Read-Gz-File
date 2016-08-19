package testO;
        import org.apache.commons.io.IOUtils;
        import java.io.*;
        import java.util.*;
        import org.apache.hadoop.fs.*;
        import org.apache.hadoop.conf.*;
        import org.apache.hadoop.io.compress.CompressionCodec;
        import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Program reads the contents of gz file and writes the data out
 *
 */
public class App 
{

    public static void main( String[] args ) throws Exception
    {
        System.out.print("arg[0]"+args[0]);
        Path pt = new Path("hdfs://LBDEV3NN/dw/dataeng/cdc/reported_lead/2016/07/21/16/15/part-m-00000.gz");
        readLines(pt, new Configuration());
    }

    public static void readLines(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return;
        List<String> results = new ArrayList<String>();
        for(FileStatus item: items) {
            if(item.getPath().getName().startsWith("_")) {
                continue;
            }
            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            }
            else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            String[] resulting = raw.split("\n");
            for(String str: raw.split("\n")) {
                for(String s1: str.split("\u0001")) {
                    System.out.println(s1);
                }

            }
        }

    }

}
