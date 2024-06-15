import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CalculatePi {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: CalculatePi <inputFile>");
            System.exit(1);
        }

        String inputFile = args[0];

        try (Stream<String> lines = Files.lines(Paths.get(inputFile))) {
            long insideCount = 0;
            long totalCount = 0;

            for (String line : (Iterable<String>) lines::iterator) {
                if (line.contains("Inside")) {
                    insideCount++;
                }
                totalCount++;
            }

            double piEstimate = 4.0 * insideCount / totalCount;
            System.out.println("Estimated value of Pi: " + piEstimate);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
