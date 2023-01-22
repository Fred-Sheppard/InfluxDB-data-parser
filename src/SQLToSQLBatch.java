import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class SQLToSQLBatch {

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
            if (!speedResults.next()) {
                break;
            }
            do {
                DataPoint point = new DataPoint();
                // SQL indexes from 1
                point.setEpoch(speedResults.getLong(1));
                point.setSpeed(speedResults.getDouble(2));
                dataPoints.add(point);
                entries++;
            } while (speedResults.next());

            ResultSet directionResults = statement.executeQuery(directionQuery);
            if (!directionResults.next()) {
                break;
            }
            int i = 0;
            do {
                DataPoint point = dataPoints.get(i);
                // SQL indexes from 1
                point.setDirection(directionResults.getDouble(1));
                i++;
            } while (directionResults.next());

            for (DataPoint point : dataPoints) {
                builder.append(point);
            }
            int length = builder.length();
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