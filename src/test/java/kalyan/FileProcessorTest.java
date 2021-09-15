package kalyan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author debal
 */
public class FileProcessorTest {

    @Test
    public void testFileCache() throws IOException {
        final File testFile = new File("src/test/resources/test-data.txt");
        final List<String> output = FileProcessor.from(testFile)
                .topTags(2);
        assertEquals(2, output.size());
        Assertions.assertArrayEquals(new String[]{"tag3=> fname6,fname7", "tag1=> fname1,fname4"}, output.toArray());
    }

    @Test
    public void testFileCacheWithNoMatch() throws IOException {
        final File testFile = new File("src/test/resources/test-data.txt");
        final List<String> output = FileProcessor.from(testFile)
                .topTags(10);
        assertTrue(output.isEmpty());
    }

}