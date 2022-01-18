package fr.iiil.bigdata.spark;

import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for simple SparkApp.
 */
public class SparkAppTest {
    String inputFilePathStr = ConfigFactory.load().getString("3il.path.input");
    String outputFolderPathStr = ConfigFactory.load().getString("3il.path.output");
    FileSystem hdfs;

    @Test
    public void shouldReadFile() throws IOException {
        SparkApp.main(new String[0]);
        assertThat(
                hdfs.listStatus(
                        new Path(outputFolderPathStr)
                ).length
        ).isGreaterThan(0);
    }

    @Before
    public void setup() throws IOException {
        Configuration hadoopConf = new Configuration();
        String localInputFileStr = "src/test/resources/data/input/bigdatadefinition.txt";
        FileSystem fs = FileSystem.getLocal(hadoopConf);
        hdfs = FileSystem.get(hadoopConf);
        Path inputFilePath = new Path(inputFilePathStr);
        Path inputFolderPath =  inputFilePath.getParent();
        hdfs.mkdirs(inputFolderPath);
        hdfs.copyFromLocalFile(false, true, new Path(localInputFileStr), inputFolderPath);
        cleanup();
    }

    @After
    public void tearDown(){
        cleanup();
    }

    public void cleanup(){
        try{

            hdfs.delete(new Path(outputFolderPathStr), true);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
