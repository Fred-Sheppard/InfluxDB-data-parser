import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Query;
import org.json.JSONObject;

import java.io.*;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Class to batch read an Influx database, and print the mean of the data at hourly intervals to a file.
 */
class InfluxMean {

    /**
     * The query to execute to retrieve each datapoint.
     */
    private static final String ORIGINAL_QUERY = "SELECT * FROM \"autogen\".\"%s\" WHERE \"source\"='derived-data'";
    /**
     * The amount of values to query in a single call. e.g. (limit 50).
     */
    private static int LIMIT_PER_CALL;
    /**
     * Formatter to allow InfluxDB times to be inputted.
     */
    private static DateTimeFormatter MY_PATTERN;

    public static void main(String[] args) throws RuntimeException, IOException {
        // The path to the jar file
        File jarPath = new File(InfluxMean.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        final String PATH;
        if (jarPath.toString().contains(".jar")) {
            // If being run from a jar file
            PATH = jarPath.getParentFile().getAbsolutePath() + "/";
        } else {
            // Run from inside IDE
            PATH = "";
        }
        // Load config file
        String json = "";
        try {
            json = new String(Files.readAllBytes(Paths.get(PATH + "config/config.json")));
        } catch (NoSuchFileException e) {
            System.err.println("No config file found. File should be found at: currentDir/config/config.json");
            System.exit(1);
        }
        JSONObject config = new JSONObject(json);
        String database = config.getString("Database");
        LIMIT_PER_CALL = config.getInt("LimitPerCall");
        //noinspection SpellCheckingInspection
        MY_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'");

        // Connect to database using try-with-resources
        try (InfluxDB influx = InfluxDBFactory.connect(config.getString("Url"),
                config.getString("User"), config.getString("Password"))) {
            influx.setDatabase(database);
            // What datapoints should be queried
            JSONObject datapoints = config.getJSONObject("Metrics");
            File output = new File(PATH + "output/");
            // If there isn't already an output folder
            if (!output.exists()) {
                boolean dirWasCreated = output.mkdir();
                // If the output directory can't be created
                if (!dirWasCreated) {
                    System.err.println("Directory output/ could not be created. Exiting");
                    System.exit(2);
                }
            }
            // Loop through datapoints in config.json
            System.out.println();
            for (String key : datapoints.keySet()) {
                JSONObject datapoint = datapoints.getJSONObject(key);
                String outputFile = "output" + "/" + key + ".txt";
                PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
                // If the program is interrupted, flush and save the file as-is
                Thread shutdown = new Thread(() -> {
                    writer.flush();
                    writer.close();
                    System.out.println("\nUnexpected shutdown, flushing to file");
                });
                Runtime.getRuntime().addShutdownHook(shutdown);
                findHourlyMeans(influx, writer, datapoint.getString("Path"), datapoint.getDouble("cFactor"));
                writer.flush();
                writer.close();
                Runtime.getRuntime().removeShutdownHook(shutdown);
            }
            System.out.println("\nProcess Completed. Exiting");
            
        } catch (InfluxDBIOException e) {
            // Any errors to do with the database will throw InfluxDBIOException
            // This method of handling allows the underlying cause to be discovered
            // Nested exception handling learned from ChatGPT
            try {
                throw (e.getCause());
            } catch (ConnectException connectException) {
                System.err.printf("Could not connect to the database at %s. " +
                        "Verify that the url, username and password are correct, " +
                        "and that the database is running.", config.getString("Url"));
            } catch (SocketTimeoutException socketTimeoutException) {
                System.err.println("Database query timed out. Try reducing the batch rate in config/config.json");
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    /**
     * Finds and logs the hourly means for the given datapoint in a database.
     * The database is read in batches by reading the datetime of the last entry of the last query.
     *
     * @param influx    The InfluxDB object associated with the database
     * @param writer    The PrintWriter object to write to
     * @param datapoint The datapoint to query
     * @param cFactor   The conversion factor to multiply values by e.g. m/s -> knts
     */
    private static void findHourlyMeans(InfluxDB influx, PrintWriter writer, String datapoint, double cFactor) {
        int totalCounter = 0;
        // Used for batching
        OffsetDateTime previousFinalTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z");
        OffsetDateTime currentHour = OffsetDateTime.from(previousFinalTime);
        double total = 0;
        int counter = 0;
        while (true) {
            String query = String.format(
                    "%s and time > '%s' limit %d",
                    String.format(ORIGINAL_QUERY, datapoint),
                    previousFinalTime.format(MY_PATTERN),
                    LIMIT_PER_CALL);
            // Query the database
            var values = InfluxReader.getValues(influx.query(new Query(query)));
            // If there are no entries, we are finished
            if (values == null) {
                break;
            }
            for (List<Object> value : values) {
                try {
                    /*
                    Get the time at the current entry.
                    If it's more than an hour from the current time we are outputting to:
                        Save and output the current average.
                        Start outputting to the next hour.
                     */
                    String timeString = (String) value.get(0);
                    OffsetDateTime influxTime = OffsetDateTime.parse(timeString);
                    OffsetDateTime influxHours = influxTime.withMinute(0).withSecond(0).withNano(0);
                    if (Duration.between(currentHour, influxHours).toHours() >= 1) {
                        writer.println(currentHour + "," + total / counter);
                        total = 0;
                        counter = 0;
                        currentHour = influxHours;
                    }
                    // Multiply the value by our conversion factor
                    double metric = (Double) value.get(4) * cFactor;
                    total += metric;
                    counter++;
                    totalCounter++;
                } catch (NullPointerException e) {
                    // Some values may be null
                    // Just skip these
                }
            }
            // Progress counter
            System.out.print("\r" + datapoint + ": " + totalCounter);

            // Get the last time that was called
            // This will be the starting point for the next batch
            String timeString = (String) values.get(values.size() - 1).get(0);
            previousFinalTime = OffsetDateTime.parse(timeString);
        }
        System.out.println(". Finished");
    }
}