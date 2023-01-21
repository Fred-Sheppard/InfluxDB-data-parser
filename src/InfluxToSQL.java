import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to generate an SQL command to reproduce an InfluxDB database.
 */
public class InfluxToSQL {

    public static void main(String[] args) {
        String query = "select * from \"autogen\".\"environment.wind.speedApparent\"";
        try (InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "root", "0")) {
            influx.setDatabase("Influx 1");
            var values = getValues((influx.query(new Query(query))).getResults());

            StringBuilder builder = new StringBuilder("INSERT INTO data (Epoch, WindSpeed) VALUES\n");
            for (var value : values) {
                String timeString = ((String) value.get(0));
                long time = OffsetDateTime.parse(timeString).toEpochSecond();
                double windSpeed = ((Double) value.get(3));
                builder.append("(");
                builder.append(time);
                builder.append(",");
                builder.append(windSpeed);
                builder.append("),\n");
            }
            int length = builder.length();
            builder.delete(length - 2, length);
            builder.append(";");
            System.out.println(builder);
        }
    }

    private static ArrayList<List<Object>> getValues(List<Result> results) {
        ArrayList<List<Object>> appendList = new ArrayList<>();
        for (Result result : results) {
            for (Series series : result.getSeries()) {
                appendList.addAll(series.getValues());
            }
        }
        return appendList;
    }
}