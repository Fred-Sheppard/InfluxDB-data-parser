import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

public class Main {

    static String query = "select * from testing";
//  static String query = "select value from \"autogen\".\"environment.wind.speedApparent\"";

    public static void main(String[] args) {
        try (InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "root", "0")) {
            influx.setDatabase("Influx 1");
            // For every value in the result
//            influx.query(new Query(query)).getResults()
//                    .forEach(r -> r.getSeries().forEach(s -> s.getValues().forEach(value -> {
//                        System.out.println(value);
//                        System.out.print("");
//                    })));
            var values = influx.query(new Query(query))
                    .getResults().get(0).getSeries().get(0).getValues();
            for (var v : values) {
                System.out.println(v);
            }
        }
    }
}