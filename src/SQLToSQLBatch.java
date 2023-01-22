import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Class to combine two databases with a large number of entries
 */
public class SQLToSQLBatch {

    /**
     * The amount of entries to query with every iteration
     */
    static final int LIMIT_PER_CALL = 100000;

    public static void main(String[] args) throws SQLException {
        InfluxToSQLBatch.validateContinue();
        Statement statement = DriverManager.getConnection(
                        "jdbc:mariadb://localhost:3307/Boat",
                        "root",
                        "1234")
                .createStatement();
        clearTable(statement);
        long previousFinalTime = 0L;

        int batches = 0;
        int entries = 0;
        while (true) {
            StringBuilder builder = new StringBuilder(
                    "INSERT INTO aggregate (Epoch, WindDir, WindSpeed) VALUES\n");
            String speedQuery = String.format("select Epoch, WindSpeed from speed where Epoch > %d limit %d",
                    previousFinalTime, LIMIT_PER_CALL);
            String directionQuery = String.format("select WindDir from direction where Epoch > %d limit %d",
                    previousFinalTime, LIMIT_PER_CALL);
            ResultSet speedResults = statement.executeQuery(speedQuery);
            ArrayList<DataPoint> dataPoints = new ArrayList<>();
            // If there are no more results, we are at the end of the database
            if (!speedResults.next()) {
                break;
            }
            // Do-while due to call of speedResults.next() above
            // https://javarevisited.blogspot.com/2016/10/how-to-check-if-resultset-is-empty-in-Java-JDBC.html#axzz7mueiayIp
            do {
                DataPoint point = new DataPoint();
                // SQL indexes from 1
                point.setEpoch(speedResults.getLong(1));
                point.setSpeed(speedResults.getDouble(2));
                dataPoints.add(point);
                entries++;
            } while (speedResults.next());

            ResultSet directionResults = statement.executeQuery(directionQuery);

            for (int i = 0; directionResults.next(); i++) {
                DataPoint point = dataPoints.get(i);
                // SQL indexes from 1
                point.setDirection(directionResults.getDouble(1));
            }

            dataPoints.forEach(builder::append);
            int length = builder.length();
            // Replace final ",\n" with  ";"
            builder.delete(length - 2, length);
            builder.append(";");
            try {
                statement.executeUpdate(builder.toString());
            } catch (SQLException e) {
                System.out.println(builder);
                throw e;
            }

            // Get the last time that was called
            // This will be the starting point for the next batch
            previousFinalTime = dataPoints.get(dataPoints.size() - 1).getEpoch();
            System.out.print("\r" + entries);
            batches++;
        }
        System.out.println("\rBatches: " + batches);
        System.out.println("Entries: " + entries);
    }

    /**
     * Clears the Boat/aggregate SQL table by dropping it and recreating it.
     *
     * @param statement Statement used to execute the query
     */
    private static void clearTable(Statement statement) throws SQLException {
        System.out.println("Dropping table");
        statement.executeUpdate("DROP TABLE aggregate");
        System.out.println("Recreating table");
        System.out.println("Querying...");
        statement.executeUpdate("""
                create table aggregate
                (
                    Epoch     bigint not null
                        primary key,
                    WindDir   float  null,
                    WindSpeed float  null
                );
                """);
    }
}

/**
 * Class to store time, direction and speed to be inputted to a single SQL database
 */
class DataPoint {

    private long epoch;
    private double direction;
    private double speed;

    DataPoint() {
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String toString() {
        return String.format("(%d, %f, %f),\n", epoch, direction, speed);
    }
}