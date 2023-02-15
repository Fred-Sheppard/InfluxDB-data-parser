import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;

public class InfluxReader {

    /**
     * Returns a list of values from an Influx result set
     *
     * @param results The result of an Influx query
     * @return A new flat-packed list of values from the query
     */
    public static ArrayList<List<Object>> getValues(QueryResult results) {
        ArrayList<List<Object>> appendList = new ArrayList<>();
        for (QueryResult.Result result : results.getResults()) {
            List<QueryResult.Series> allSeries = result.getSeries();
            if (allSeries == null) {
                return null;
            }
            for (QueryResult.Series series : result.getSeries()) {
                appendList.addAll(series.getValues());
            }
        }
        return appendList;
    }
}
