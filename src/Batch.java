import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Batch {

    static int limitPerCall = 10;

    public static void main(String[] args) {
        try (InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "root", "0")) {
            influx.setDatabase("Influx 1");
            OffsetDateTime previousFinalTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z");
            String originalQuery = "select * from \"autogen\".\"environment.wind.speedTrue\"";
            @SuppressWarnings("SpellCheckingInspection")
            DateTimeFormatter myPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'");
            int counter = 0;
            int iterator = 0;
            while (iterator < 10) {
                try {
                    System.out.println();
                    String query = String.format(
                            "%s where time > '%s' limit %d",
                            originalQuery, previousFinalTime.format(myPattern), limitPerCall);
                    var values = getValues(influx.query(new Query(query)));
                    for (List<Object> value : values) {
                        // Todo add to SQL insert query
                        System.out.println(value);
                        counter++;
                    }

                    // Get the last time that was called
                    // This will be the starting point for the next batch
                    String timeString = (String) values.get(values.size() - 1).get(0);
                    previousFinalTime = OffsetDateTime.parse(timeString);
                    iterator++;
                } catch (Exception e) {
                    System.out.println("Finished");
                    break;
                }
            }
            System.out.println(counter);
        }
    }

    private static ArrayList<List<Object>> getValues(QueryResult results) {
        ArrayList<List<Object>> appendList = new ArrayList<>();
        for (Result result : results.getResults()) {
            for (Series series : result.getSeries()) {
                appendList.addAll(series.getValues());
            }
        }
        return appendList;
    }

}