package com.appian;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;

public class QueryTimer {

  public static void main(String[] args) throws Exception {
    // Read configs
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(System.in));
    // configs

    System.out.println("Provide a JDBC connection string");
    String jdbcConnectionString = "";
    while (isEmpty(jdbcConnectionString)) {
      jdbcConnectionString = reader.readLine();
    }
    System.out.println("jdbcConnectionString: " + jdbcConnectionString);

    System.out.println("Provide a username");
    String username = "";
    while (isEmpty(username)) {
      username = reader.readLine();
    }
    System.out.println("username: " + username);

    System.out.println("Provide a password");
    String password = "";
    while (isEmpty(password)) {
      password = reader.readLine();
    }
    String passwordStarred = "";
    for (char c : password.toCharArray()) {
      passwordStarred += "*";
    }
    System.out.println("password: " + passwordStarred);

    System.out.println("Provide a query that will be executed");
    String repeatedQuery = "";
    while(isEmpty(repeatedQuery)) {
      repeatedQuery = reader.readLine();
    }
    System.out.println("repeatedQuery: " + repeatedQuery);

    System.out.println("Provide a number of threads that will each execute the query");
    int numThreads = -1;
    while (numThreads < 1) {
      try {
        numThreads = Integer.valueOf(reader.readLine());
      } catch (Exception e) {
      }
    }
    System.out.println("numTheads: " + numThreads);

    System.out.println("Provide a thread frequency in seconds at which each thread will make repeated queries");
    int threadFrequencySeconds = -1;
    while (threadFrequencySeconds < 1) {
      try {
        threadFrequencySeconds = Integer.valueOf(reader.readLine());
      } catch (Exception e) {
      }
    }

    System.out.println("Provide a initialSize (default: 0, The initial number of connections that are created when the pool is started.)");
    int initialSize = getIntOrDefault(reader, 0);
    System.out.println("initialSize: " + initialSize);

    System.out.println("Provide a maxTotal (default: 200)");
    int maxTotal = getIntOrDefault(reader, 200);
    System.out.println("maxTotal: " + maxTotal);

    System.out.println("Provide a maxIdle (default: 200)");
    int maxIdle = getIntOrDefault(reader, 200);
    System.out.println("maxIdle: " + maxIdle);

    System.out.println("Provide a minIdle (default: 5)");
    int minIdle = getIntOrDefault(reader, 5);
    System.out.println("minIdle: " + minIdle);

    System.out.println("Provide a maxWaitMillis (default: 1000, -1=indefinitely wait, The maximum number of milliseconds that the pool will wait (when there are no available connections) for a connection to be returned before throwing an exception)");
    int maxWaitMillis = getIntOrDefault(reader, 1000);
    System.out.println("maxWaitMillis: " + maxWaitMillis);

    // ---

    System.out.println("Provide a validation query (default: <empty> (underlying isValid() will be used instead of a query), The SQL query that will be used to validate connections from this pool before returning them to the caller. If specified, this query MUST be an SQL SELECT statement that returns at least one row. If not specified, connections will be validation by calling the isValid() method.)");
    String validationQuery = getStringOrDefault(reader, null);
    System.out.println("validationQuery: " + validationQuery);

    System.out.println("Provide a validation query timeout in seconds (default: 5, 0=no timeout/infinite, the timeout in seconds before connection validation queries fail)");
    int validationQueryTimeoutSeconds = getIntOrDefault(reader, 5);
    System.out.println("validationQueryTimeoutSeconds: " + validationQueryTimeoutSeconds);

    System.out.println("Provide a value of testOnCreate (default: false)");
    boolean testOnCreate = getBooleanOrDefault(reader, false);
    System.out.println("testOnCreate: " + testOnCreate);

    System.out.println("Provide a value of testOnBorrow (default: true)");
    boolean testOnBorrow = getBooleanOrDefault(reader, true);
    System.out.println("testOnBorrow: " + testOnBorrow);

    System.out.println("Provide a value of testOnReturn (default: false)");
    boolean testOnReturn = getBooleanOrDefault(reader, false);
    System.out.println("testOnReturn: " + testOnReturn);

    System.out.println("Provide a value of testWhileIdle (default: false)");
    boolean testWhileIdle = getBooleanOrDefault(reader, false);
    System.out.println("testWhileIdle: " + testWhileIdle);

    System.out.println("Provide a value of timeBetweenEvictionRunsMillis (default: 450000, -1=Don't use evictor, The number of milliseconds to sleep between runs of the idle object evictor thread. When non-positive, no idle object evictor thread will be run.)");
    int timeBetweenEvictionRunsMillis = getIntOrDefault(reader, 450000);
    System.out.println("timeBetweenEvictionRunsMillis: " + timeBetweenEvictionRunsMillis);

    int MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT = 900000;
    System.out.println("Provide a value of minEvictableIdleTimeMillis (default: " + MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT + ", The minimum amount of time an object may sit idle in the pool before it is eligible for eviction by the idle object evictor (if any).)");
    int minEvictableIdleTimeMillis = getIntOrDefault(reader, MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT);
    System.out.println("minEvictableIdleTimeMillis: " + minEvictableIdleTimeMillis);

    System.out.println("Provide a value for maxConnLifetimeMillis (default: -1, The maximum lifetime in milliseconds of a connection. After this time is exceeded the connection will fail the next activation, passivation or validation test. A value of zero or less means the connection has an infinite lifetime.)");
    int maxConnLifetimeMillis = getIntOrDefault(reader, -1);
    System.out.println("maxConnLifetimeMillis: " + maxConnLifetimeMillis);

    BasicDataSource dataSource = new BasicDataSource();

    dataSource.setUrl(jdbcConnectionString);
    dataSource.setUsername(username);
    dataSource.setPassword(password);

    if (!isEmpty(validationQuery)) {
      dataSource.setValidationQuery(validationQuery);
    }
    dataSource.setValidationQueryTimeout(validationQueryTimeoutSeconds);
    dataSource.setTestOnCreate(testOnCreate);
    dataSource.setTestOnBorrow(testOnBorrow);
    dataSource.setTestOnReturn(testOnReturn);
    dataSource.setTestWhileIdle(testWhileIdle);
    dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
    dataSource.setNumTestsPerEvictionRun(maxIdle); // mimics AE
    dataSource.setAccessToUnderlyingConnectionAllowed(true); // mimics AE

    dataSource.setInitialSize(initialSize);
    dataSource.setMaxTotal(maxTotal);
    dataSource.setMaxIdle(maxIdle);
    dataSource.setMinIdle(minIdle);
    dataSource.setMaxWaitMillis(maxWaitMillis);

    int TRANSACTION_READ_COMMITTED = 2;
    dataSource.setDefaultTransactionIsolation(TRANSACTION_READ_COMMITTED); // mimics AE

    Class cls = Class.forName("org.mariadb.jdbc.Driver");
    Driver driver = (Driver)cls.newInstance();
    DriverManager.registerDriver(driver);

    dataSource.setDriver(driver);

    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor
        = new ScheduledThreadPoolExecutor(1);
    scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> System.out.println(printDataSourceStats(dataSource)), 0, 1, TimeUnit.SECONDS);

    final String repeatedQueryFinal = repeatedQuery;
    ScheduledThreadPoolExecutor connectionThreadPool = new ScheduledThreadPoolExecutor(numThreads);
    for (int i = 0; i < numThreads; i++) {
      connectionThreadPool.scheduleAtFixedRate(() -> {
        try (Connection conn = timing("Getting connection", () -> dataSource.getConnection());
             PreparedStatement stmt = timing("Preparing statement", () -> conn.prepareStatement(repeatedQueryFinal))) {
          ResultSet rs = timing("Executing query", () -> stmt.executeQuery());
//          timing("fetch resultset metadata", () -> displayResultSetMetadata(rs.getMetaData()));
//          while (rs.next()) {
//            timing("fetch resultset row", () -> displayResultSetRow(rs));
//          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 0, threadFrequencySeconds, TimeUnit.SECONDS);
    }

//
//    if (args.length < 3) {
//      System.out.println("Usage:");
//      System.out.println("   QueryTimer <driverClass> <connectionUrl> <user>");
//      System.out.println("Examples");
//      System.out.println("   QueryTimer com.mysql.jdbc.Driver jdbc:mysql://localhost:3306/AppianDB appian");
//      System.out.println("   QueryTimer oracle.jdbc.OracleDriver jdbc:oracle:thin:@172.17.0.1:1521:citest appian");
//      System.exit(1);
//    }
//
//
//
//    final String driverName = args[0];
//    final String url = args[1];
//    final String user = args[2];
//
//    Class cls = Class.forName(driverName);
//    Driver driver = (Driver)cls.newInstance();
//    DriverManager.registerDriver(driver);
//
//    String password = args.length == 4 ? args[3] : ask("Password: ");
//    final Connection connection = timing("creating connection", () -> DriverManager.getConnection(url, user, password));
//
//
//
//    StringBuffer query = new StringBuffer();
//    while(true) {
//      String line = ask(query.length() > 0 ? "" : "Query> ");
//      if (line.equalsIgnoreCase("quit")) {
//        break;
//      }
//      query.append(line).append(' ');
//      if (!line.trim().endsWith(";")) {
//        continue;
//      }
//      long start = System.currentTimeMillis();
//      String stmt = query.toString();
//      query = new StringBuffer();
//      try (PreparedStatement pstmt = timing("prepare statement", () -> connection.prepareStatement(stmt));
//           ResultSet rs = timing("execute query", () -> pstmt.executeQuery())) {
//        timing("fetch resultset metadata", () -> displayResultSetMetadata(rs.getMetaData()));
//        while (rs.next()) {
//          timing("fetch resultset row", () -> displayResultSetRow(rs));
//        }
//      }
//      System.out.println("Total Query Execution time: " + (System.currentTimeMillis() - start) + " msecs");
//    }
//
//    connection.close();
  }

  private static String ask(String prompt) throws IOException {
    System.out.print(prompt);
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    return br.readLine();
  }

  private static <T> T timing(String label, Callable<T> callable) throws Exception {
    long start = System.currentTimeMillis();
    try {
      return callable.call();
    } finally {
      System.out.println(label + " took " + (System.currentTimeMillis() - start) + " msecs");
    }
  }

  private static boolean displayResultSetMetadata(ResultSetMetaData rsMd) throws SQLException {
    int nCols = rsMd.getColumnCount();
    for (int i = 1; i <= nCols; i++) {
      System.out.print(rsMd.getColumnName(i));
      System.out.print(",");
    }
    System.out.println();
    return true;
  }

  private static boolean displayResultSetRow(ResultSet rs) throws SQLException {
    int nCols = rs.getMetaData().getColumnCount();
    for (int i = 1; i <= nCols; i++) {
      System.out.print(rs.getString(i));
      System.out.print(",");
    }
    System.out.println();
    return true;
  }

  private static boolean isEmpty(String s) {
    return s == null || s.length() == 0;
  }

  private static boolean getBooleanOrDefault(BufferedReader reader, boolean defaultValue) throws IOException {
    String s = reader.readLine();
    if (isEmpty(s)) {
      s = String.valueOf(defaultValue);
    }

    return Boolean.valueOf(s);
  }

  private static String getStringOrDefault(BufferedReader reader, String defaultValue) throws IOException {
    String s = reader.readLine();
    if (isEmpty(s)) {
      s = defaultValue;
    }

    return s;
  }

  private static int getIntOrDefault(BufferedReader reader, int defaultValue) throws IOException {
    String s = reader.readLine();
    if (isEmpty(s)) {
      s = String.valueOf(defaultValue);
    }

    return Integer.valueOf(s);
  }

  private static String printDataSourceStats(BasicDataSource bds) {
    try {
      Method getConnectionPoolMethod = BasicDataSource.class.getDeclaredMethod("getConnectionPool");
      getConnectionPoolMethod.setAccessible(true);

      GenericObjectPool<PoolableConnection> pool = (GenericObjectPool<PoolableConnection>) getConnectionPoolMethod.invoke(bds);

      return "Active/Idle/MaxTotal/Created/Borrowed/DestroyedBorrow/DestroyedEvictor/Destroyed: " + bds.getNumActive() +
          "\t" + bds.getNumIdle() + "\t" + bds.getMaxTotal() + "\t" + pool.getCreatedCount() + "\t" + pool.getBorrowedCount() +
          "\t" + pool.getDestroyedByBorrowValidationCount() + "\t" + pool.getDestroyedByEvictorCount() + "\t" + pool.getDestroyedCount();
    } catch (Exception e) {
      return "";
    }
  }
}
