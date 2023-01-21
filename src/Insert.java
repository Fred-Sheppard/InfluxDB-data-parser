import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class Insert {
    private static final String DB_NAME = "One";
    private static InfluxDB influxDB;

    public static void main(String[] args) {
        // Connect to InfluxDB
        influxDB = InfluxDBFactory.connect("http://localhost:8086");

        influxDB.setDatabase(DB_NAME);

        for (int i = 0; i < 10; i++) {
            setData();
        }
    }

    private static void setData() {
        BatchPoints batchPoints = BatchPoints
                .database(DB_NAME)
                .tag("async", "true")
                .retentionPolicy("autogen")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();

        Point point1 = Point.measurement("temperature")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("value", Math.random() * 30)
                .build();
        Point point2 = Point.measurement("humidity")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("value", Math.random() * 100)
                .build();

        batchPoints.point(point1);
        batchPoints.point(point2);
        influxDB.write(batchPoints);
    }
}