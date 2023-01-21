import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Class to upload a large amount of data from InfluxDB to SQL
 */
public class Batch {

    static final String DATABASE = "Influx 1";
    static final int LIMIT_PER_CALL = 10;
    static final String ORIGINAL_QUERY = """
            SELECT * FROM "autogen"."environment.wind.speedTrue" WHERE "source"='derived-data'""";


    public static void main(String[] args) throws SQLException, RuntimeException, IOException {
        // This is a heavy operation that also clears the existing data from SQL
        // We need to ensure the user hasn't run this by error
        System.out.println("This will clear the database. Proceed?");
        Scanner input = new Scanner(System.in);
        boolean valid = false;
        while (!valid) {
            System.out.println("Y/N?");
            String s = input.nextLine().toLowerCase();
            if (s.equals("y")) {
                valid = true;
            } else if (s.equals("n")) {
                System.out.println("Cancelling");
                System.exit(1);
            }
        }

        PrintWriter logger = new PrintWriter(new BufferedWriter(new FileWriter("log.txt")));
        try (InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "root", "0")) {
            influx.setDatabase(DATABASE);
            OffsetDateTime previousFinalTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z");
            @SuppressWarnings("SpellCheckingInspection")
            DateTimeFormatter myPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'");
            Statement sqlStatement;
            sqlStatement = DriverManager.getConnection(
                            "jdbc:mariadb://localhost:3307/Boat",
                            "root",
                            "1234")
                    .createStatement();
            clearTable(sqlStatement);

            int counter = 0;
            while (true) {
                StringBuilder builder = new StringBuilder("INSERT INTO data (Epoch, WindSpeed) VALUES\n");
                try {
                    String query = String.format(
                            "%s and time > '%s' limit %d",
                            ORIGINAL_QUERY, previousFinalTime.format(myPattern), LIMIT_PER_CALL);
                    var values = getValues(influx.query(new Query(query)));
                    if (values == null) {
                        break;
                    }
                    for (List<Object> value : values) {
                        try {
                            String timeString = (String) value.get(0);
                            OffsetDateTime time = OffsetDateTime.parse(timeString);
                            // Get every fifth value
                            if (counter++ % 5 != 0) {
                                continue;
                            }
                            double windSpeed = ((Double) value.get(4));
                            builder.append("(");
                            builder.append(time.toEpochSecond());
                            builder.append(",");
                            builder.append(windSpeed);
                            builder.append("),\n");
                        } catch (Exception e) {
                            // Most commonly caused by null values in the database
                            logger.println(counter + ": " + value);
                        }
                    }
                    System.out.print("\r" + counter);
                    int length = builder.length();
                    builder.delete(length - 2, length);
                    builder.append(";");
                    sqlStatement.executeUpdate(builder.toString());

                    // Get the last time that was called
                    // This will be the starting point for the next batch
                    String timeString = (String) values.get(values.size() - 1).get(0);
                    previousFinalTime = OffsetDateTime.parse(timeString);
                } catch (SQLException e) {
                    System.out.println(builder);
                    throw (e);
                }
            }

            logger.flush();
            logger.close();
            System.out.println("\nFinished");
        }
    }

    /**
     * Returns a list of values from an Influx result set
     *
     * @param results The result of an Influx query
     * @return A new flat-packed list of values from the query
     */
    private static ArrayList<List<Object>> getValues(QueryResult results) {
        ArrayList<List<Object>> appendList = new ArrayList<>();
        for (Result result : results.getResults()) {
            List<Series> allSeries = result.getSeries();
            if (allSeries == null) {
                return null;
            }
            for (Series series : result.getSeries()) {
                appendList.addAll(series.getValues());
            }
        }
        return appendList;
    }

    /**
     * Clears the Boat/data SQL table by dropping it and recreating it.
     *
     * @param sqlStatement Statement used to execute the query
     */
    private static void clearTable(Statement sqlStatement) throws SQLException {
        System.out.println("Dropping table");
        sqlStatement.executeUpdate("DROP TABLE DATA");
        System.out.println("Recreating table");
        System.out.println("Querying...");
        sqlStatement.executeUpdate("""
                create table data
                (
                    ID        int auto_increment
                        primary key,
                    Epoch     bigint null,
                    WindDir   int    null,
                    WindGust  float  null,
                    WindSpeed float  null
                );
                """);
    }
}