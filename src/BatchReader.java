import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BatchReader {


    private final InfluxDB influx;
    private final String query;
    private long callLimit;
    @SuppressWarnings("SpellCheckingInspection")
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'");
    private OffsetDateTime startTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z");
    private int index;
    ArrayList<List<Object>> rows;

    public BatchReader(InfluxDB influx, String query, long callLimit) {
        this.influx = influx;
        this.query = query;
        this.callLimit = callLimit;
    }

    public void setCallLimit(long limit) {
        callLimit = limit;
    }

    public void setStartTime(OffsetDateTime dateTime) {
        startTime = dateTime;
    }

    public void next() {
        if (rows.size() >= index) {
            rows = batchRead();
        }
        List<Object> row = rows.get(index);
    }

    public ArrayList<List<Object>> batchRead() {
        index = 0;
        String batchQuery = String.format(
                "%s and time > '%s' limit %d",
                query,
                startTime.format(timeFormatter),
                callLimit);
        // Query the database
        // If there are no entries, we are finished

        rows = InfluxReader.getValues(influx.query(new Query(batchQuery)));
        if (rows == null) return null;
        // Get the last time that was called
        // This will be the starting point for the next batch
        String timeString = (String) rows.get(rows.size() - 1).get(0);
        startTime = OffsetDateTime.parse(timeString);
        return rows;
    }
}
