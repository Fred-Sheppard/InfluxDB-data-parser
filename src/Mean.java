import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;

public class Mean {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("output.txt"));
        double mean = Stream.of(reader.readLine().split(","))
                .mapToDouble(Double::valueOf)
                .average()
                .orElse(-1);
        System.out.println(mean);
    }
}
