package kalyan;

import static java.lang.Integer.parseInt;
import static java.nio.file.Files.lines;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author debal
 */
public class FileProcessor {
    final Map<String, RecordListWithSizeAndNums> cache;
    final IntFunction<Predicate<Map.Entry<String, RecordListWithSizeAndNums>>> filterCache =
        numFiles -> e -> e.getValue().numElements() == numFiles;

    static final class Record {
        final String fileName;
        final int fileSize;
        final String tag;

        private Record(String fileName, int fileSize, String tag) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.tag = tag;
        }

        public static Record fromLine(String line) {
            String[] elems = line.split(",");
            return new Record(elems[0].trim(), parseInt(elems[1].trim()), elems[2].trim());
        }
    }

    static final class RecordListWithSizeAndNums {
        final List<Record> records;
        final int totalSize;

        public RecordListWithSizeAndNums(List<Record> records) {
            this.records = records;
            this.totalSize = records
                    .stream()
                    .mapToInt(r -> r.fileSize)
                    .sum();
        }

        public int numElements() {
            return records.size();
        }
    }


    private FileProcessor(Map<String, RecordListWithSizeAndNums> cache) {
        this.cache = cache;
    }

    public static FileProcessor from(File input) throws IOException {
        try(Stream<String> lines = lines(input.toPath())) {
            return new FileProcessor(lines
                    .map(Record::fromLine)
                    .collect(groupingBy(r -> r.tag, collectingAndThen(toList(), RecordListWithSizeAndNums::new))));
        }
    }

    public List<String> topTags(int numFiles) {
        final ToIntFunction<Map.Entry<String, RecordListWithSizeAndNums>> sortingFunction = e -> e.getValue().totalSize;
        return cache.entrySet()
                .stream()
                .filter(filterCache.apply(numFiles))
                .sorted(comparingInt(sortingFunction).reversed())
                .limit(numFiles)
                .map(this::prepareOutput)
                .collect(toList());

    }

    private String prepareOutput(Map.Entry<String, RecordListWithSizeAndNums> e) {
        return e.getKey() + "=> " + concatFileNames(e.getValue());
    }

    private String concatFileNames(RecordListWithSizeAndNums recordView) {
        return recordView.records
                .stream()
                .map(r -> r.fileName)
                .collect(Collectors.joining(","));
    }
}

