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
import java.util.concurrent.*;

import org.apache.commons.cli.*;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class QueryTimer {

  public static String JDBC_CONNECTION_STRING_KEY = "jdbcConnectionString";
  public static String USERNAME_KEY = "username";
  public static String PASSWORD_KEY = "password";
  public static String TEST_ON_CREATE_KEY = "testOnCreate";
  public static String TEST_ON_BORROW_KEY = "testOnBorrow";
  public static String TEST_ON_RETURN_KEY = "testOnReturn";
  public static String TEST_WHILE_IDLE_KEY = "testWhileIdle";
  public static String NUM_THREADS_KEY = "numThreads";
  public static String QUERY_FREQUENCY_SECS_KEY = "queryFrequencySecs";
  public static String INITIAL_SIZE_KEY = "initialSize";
  public static String MAX_TOTAL_KEY = "maxTotal";
  public static String MIN_IDLE_KEY = "minIdle";
  public static String MAX_IDLE_KEY = "maxIdle";
  public static String MAX_WAIT_MILLIS_KEY = "maxWaitMillis";
  public static String TIME_BETWEEN_EVICTION_RUN_MILLIS_KEY = "timeBetweenEvictionRunsMillis";
  public static String MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY = "minEvictableIdleTimeMillis";
  public static String MAX_CONN_LIFETIME_MILLIS_KEY = "maxConnLifetimeMillis";
  public static String VALIDATION_QUERY_TIMEOUT_SECONDS_KEY = "validationQueryTimeoutSeconds";
  public static String REMOVE_ABANDONED_ON_BORROW_KEY = "removeAbandonedOnBorrow";
  public static String REMOVE_ABANDONED_ON_MAINTENANCE_KEY = "removeAbandonedOnMaintenance";
  public static String RDBMS_TYPE_KEY = "rdbmsType";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(TEST_ON_CREATE_KEY, true, "The indication of whether objects will be validated after creation. If the " +
        "object fails to validate, the borrow attempt that triggered the object creation will fail. Default: false");
    options.addOption(TEST_ON_BORROW_KEY, true, "The indication of whether objects will be validated before being borrowed " +
        "from the pool. If the object fails to validate, it will be dropped from the pool, and we will attempt to borrow another. Default: true");
    options.addOption(TEST_ON_RETURN_KEY, true, "The indication of whether objects will be validated before being returned to the pool. Default: false");
    options.addOption(TEST_WHILE_IDLE_KEY, true, "The indication of whether objects will be validated by the idle object " +
        "evictor (if any). If an object fails to validate, it will be dropped from the pool. Default: false");
    options.addOption(NUM_THREADS_KEY, true, "Number of threads to use. Since each thread performs a query, this " +
        "corresponds to number of concurrent queries. Default: 1");
    options.addOption(QUERY_FREQUENCY_SECS_KEY, true, "Period/frequency with which each thread will make a query. Default: 30s");
    options.addOption(INITIAL_SIZE_KEY, true, "The initial number of connections that are created when the pool is started. Default: 0");
    options.addOption(MAX_TOTAL_KEY, true, "The maximum number of active connections that can be allocated from this pool at the same time, or negative for no limit. Default: 200");
    options.addOption(MIN_IDLE_KEY, true, "The minimum number of connections that can remain idle in the pool, without extra ones being created, or zero to create none. Default:  5");
    options.addOption(MAX_IDLE_KEY, true, "The maximum number of connections that can remain idle in the pool, without extra ones being released, or negative for no limit. Default: 200");
    options.addOption(MAX_WAIT_MILLIS_KEY, true, "The maximum number of milliseconds that the pool will wait " +
        "(when there are no available connections) for a connection to be returned before throwing an exception. Default: 1000, -1=indefinitely wait");
    options.addOption(TIME_BETWEEN_EVICTION_RUN_MILLIS_KEY, true, "The number of milliseconds to sleep between runs of the idle object evictor thread. " +
        "When non-positive, no idle object evictor thread will be run. Default: 450000, -1=Don't use evictor.");
    options.addOption(MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY, true, "The minimum amount of time an object may sit idle in the pool before it is eligible for eviction by the idle object evictor (if any).). Default: 900000");
    options.addOption(MAX_CONN_LIFETIME_MILLIS_KEY, true, "The maximum lifetime in milliseconds of a connection. " +
        "After this time is exceeded the connection will fail the next activation, passivation or validation test. " +
        "A value of zero or less means the connection has an infinite lifetime. Default: -1");
    options.addOption(VALIDATION_QUERY_TIMEOUT_SECONDS_KEY, true, "The timeout in seconds before connection validation queries fail. Default: 5, 0=no timeout/infinite");
    options.addOption(REMOVE_ABANDONED_ON_BORROW_KEY, true, "If removeAbandonedOnBorrow is true, abandoned connections are removed each time a connection is borrowed from the pool, with the additional requirements that getNumActive() > getMaxTotal() - 3; and getNumIdle() < 2");
    options.addOption(REMOVE_ABANDONED_ON_MAINTENANCE_KEY, true, "Setting removeAbandonedOnMaintenance to true removes abandoned connections on the maintenance cycle (when eviction ends). This property has no effect unless maintenance is enabled by setting timeBetweenEvictionRunsMillis to a positive value.");
    options.addOption(RDBMS_TYPE_KEY, true, "RDBMS Server type Default: MariaDB (e.g., maria, maridb, mysql, mssql, sqlserver, postgres, postgresql, pg, oracle, db2)");

    /* Required Options */
    Option jdbcConnectionStringOption = new Option(JDBC_CONNECTION_STRING_KEY, true, "");
    jdbcConnectionStringOption.setRequired(true);
    options.addOption(jdbcConnectionStringOption);

    Option usernameOption = new Option(USERNAME_KEY, true, "");
    usernameOption.setRequired(true);
    options.addOption(usernameOption);

    Option passwordOption = new Option(PASSWORD_KEY, true, "");
    passwordOption.setRequired(true);
    options.addOption(passwordOption);
    /* End Required Options */

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
       cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("ConnectionPoolTest.sh -jdbcConnectionString \"<jdbc connection string>\" -username <username> -password <password>", options);
      System.out.println(e.getMessage());
      return;
    }

    /* Required arguments */
    String jdbcConnectionString = cmd.getOptionValue(JDBC_CONNECTION_STRING_KEY);
    String username = cmd.getOptionValue(USERNAME_KEY);
    String password = cmd.getOptionValue(PASSWORD_KEY);
    String repeatedQuery;
    String validationQuery = null;
    /* End Required arguments */

    boolean testOnCreate = false;
    boolean testOnBorrow = true;
    boolean testOnReturn = false;
    boolean testWhileIdle = false;
    int numThreads = 1;
    int queryFrequencySecs = 30;
    int initialSize = 0;
    int maxTotal = 200;
    int minIdle = 5;
    int maxIdle = 200;
    int maxWaitMillis = 1000;
    int timeBetweenEvictionRunsMillis = 450000;
    int minEvictableIdleTimeMillis = 900000;
    int maxConnLifetimeMillis = -1;
    int validationQueryTimeoutSeconds = 5;
    boolean removeAbandonedOnBorrow = false;
    boolean removeAbandonedOnMaintenance = false;
    String rdbmsType = "mariadb";

    if (cmd.hasOption(TEST_ON_CREATE_KEY)) {
      testOnCreate = Boolean.valueOf(cmd.getOptionValue(TEST_ON_CREATE_KEY));
    }
    if (cmd.hasOption(TEST_ON_BORROW_KEY)) {
      testOnBorrow = Boolean.valueOf(cmd.getOptionValue(TEST_ON_BORROW_KEY));
    }
    if (cmd.hasOption(TEST_ON_RETURN_KEY)) {
      testOnReturn = Boolean.valueOf(cmd.getOptionValue(TEST_ON_RETURN_KEY));
    }
    if (cmd.hasOption(TEST_WHILE_IDLE_KEY)) {
      testWhileIdle = Boolean.valueOf(cmd.getOptionValue(TEST_WHILE_IDLE_KEY));
    }
    if (cmd.hasOption(NUM_THREADS_KEY)) {
      numThreads = Integer.valueOf(cmd.getOptionValue(NUM_THREADS_KEY));
    }
    if (cmd.hasOption(QUERY_FREQUENCY_SECS_KEY)) {
      queryFrequencySecs = Integer.valueOf(cmd.getOptionValue(QUERY_FREQUENCY_SECS_KEY));
    }
    if (cmd.hasOption(INITIAL_SIZE_KEY)) {
      initialSize = Integer.valueOf(cmd.getOptionValue(INITIAL_SIZE_KEY));
    }
    if (cmd.hasOption(MAX_TOTAL_KEY)) {
      maxTotal = Integer.valueOf(cmd.getOptionValue(MAX_TOTAL_KEY));
    }
    if (cmd.hasOption(MIN_IDLE_KEY)) {
      minIdle = Integer.valueOf(cmd.getOptionValue(MIN_IDLE_KEY));
    }
    if (cmd.hasOption(MAX_IDLE_KEY)) {
      maxIdle = Integer.valueOf(cmd.getOptionValue(MAX_IDLE_KEY));
    }
    if (cmd.hasOption(MAX_WAIT_MILLIS_KEY)) {
      maxWaitMillis = Integer.valueOf(cmd.getOptionValue(MAX_WAIT_MILLIS_KEY));
    }
    if (cmd.hasOption(TIME_BETWEEN_EVICTION_RUN_MILLIS_KEY)) {
      timeBetweenEvictionRunsMillis = Integer.valueOf(cmd.getOptionValue(TIME_BETWEEN_EVICTION_RUN_MILLIS_KEY));
    }
    if (cmd.hasOption(MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY)) {
      minEvictableIdleTimeMillis = Integer.valueOf(cmd.getOptionValue(MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY));
    }
    if (cmd.hasOption(MAX_CONN_LIFETIME_MILLIS_KEY)) {
      maxConnLifetimeMillis = Integer.valueOf(cmd.getOptionValue(MAX_CONN_LIFETIME_MILLIS_KEY));
    }
    if (cmd.hasOption(VALIDATION_QUERY_TIMEOUT_SECONDS_KEY)) {
      validationQueryTimeoutSeconds = Integer.valueOf(cmd.getOptionValue(VALIDATION_QUERY_TIMEOUT_SECONDS_KEY));
    }
    if (cmd.hasOption(REMOVE_ABANDONED_ON_BORROW_KEY)) {
      removeAbandonedOnBorrow = Boolean.valueOf(cmd.getOptionValue(REMOVE_ABANDONED_ON_BORROW_KEY));
    }
    if (cmd.hasOption(REMOVE_ABANDONED_ON_MAINTENANCE_KEY)) {
      removeAbandonedOnMaintenance = Boolean.valueOf(cmd.getOptionValue(REMOVE_ABANDONED_ON_MAINTENANCE_KEY));
    }
    if (cmd.hasOption(RDBMS_TYPE_KEY)) {
      rdbmsType = cmd.getOptionValue(RDBMS_TYPE_KEY).toLowerCase();
    }

    /* Ask for input for queries because command line will have trouble parsing args with spaces */
    String repeatedQueryInput = null;
    while (isEmpty(repeatedQueryInput)) {
      repeatedQueryInput = ask("Provide a query to repeatedly execute. This will be used by all threads at the specified frequency to make concurrent calls to the RDBMS");
    }
    repeatedQuery = repeatedQueryInput;
    
    String validationQueryInput = ask("Provide a validation query. This is the SQL query that will be used to validate connections from this pool " +
        "before returning them to the caller. If specified, this query MUST be an SQL SELECT statement that returns at " +
        "least one row. If not specified, connections will be validation by calling the isValid() method. " +
        "Default: null (underlying isValid() will be used instead of a query)\"");
    if (!isEmpty(validationQueryInput)) {
      validationQuery = validationQueryInput;
    }
    /* End ask */

    System.out.println("jdbcConnectionString: " + jdbcConnectionString);
    System.out.println("username: " + username);
    System.out.println("repeatedQuery: " + repeatedQuery);
    System.out.println("testOnCreate: " + testOnCreate);
    System.out.println("testOnBorrow: " + testOnBorrow);
    System.out.println("testOnReturn: " + testOnReturn);
    System.out.println("testWhileIdle: " + testWhileIdle);
    System.out.println("numThreads: " + numThreads);
    System.out.println("queryFrequencySecs: " + queryFrequencySecs);
    System.out.println("initialSize: " + initialSize);
    System.out.println("maxTotal: " + maxTotal);
    System.out.println("minIdle: " + minIdle);
    System.out.println("maxWaitMillis: " + maxWaitMillis);
    System.out.println("timeBetweenEvictionRunsMillis: " + timeBetweenEvictionRunsMillis);
    System.out.println("minEvictableIdleTimeMillis: " + minEvictableIdleTimeMillis);
    System.out.println("maxConnLifetimeMillis: " + maxConnLifetimeMillis);
    System.out.println("validationQueryTimeoutSeconds: " + validationQueryTimeoutSeconds);
    System.out.println("validationQuery: " + validationQuery);
    System.out.println("removeAbandonedOnBorrow: " + removeAbandonedOnBorrow);
    System.out.println("removeAbandonedOnMaintenance: " + removeAbandonedOnMaintenance);
    System.out.println("rdbmsType: " + rdbmsType);

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

    dataSource.setRemoveAbandonedOnBorrow(removeAbandonedOnBorrow);
    dataSource.setRemoveAbandonedOnMaintenance(removeAbandonedOnMaintenance);

    int TRANSACTION_READ_COMMITTED = 2;
    dataSource.setDefaultTransactionIsolation(TRANSACTION_READ_COMMITTED); // mimics AE

    String driverClass;
    switch(rdbmsType) {
      case("maria"):
      case("mariadb"):
        driverClass = "org.mariadb.jdbc.Driver";
        break;
      case("mysql"):
        driverClass = "com.mysql.cj.jdbc.Driver";
        break;
      case("oracle"):
        driverClass = "oracle.jdbc.OracleDriver";
        break;
      case("pg"):
      case("postgres"):
      case("postgresql"):
        driverClass = "org.postgresql.Driver";
        break;
      case("sqlserver"):
      case("mssql"):
        driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        break;
      default:
        throw new RuntimeException("Unsupported rdbmsType: " + rdbmsType);
    }

    Class cls = Class.forName(driverClass);
    Driver driver = (Driver)cls.newInstance();
    DriverManager.registerDriver(driver);

    dataSource.setDriver(driver);

    try (Connection testCreationConnection = timing("creating connection WITHOUT a connection pool", () -> DriverManager.getConnection(jdbcConnectionString, username, password));) {
    }

    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor
        = new ScheduledThreadPoolExecutor(1);
    scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> printDataSourceStats(dataSource), 0, 1, TimeUnit.SECONDS);

    final String repeatedQueryFinal = repeatedQuery;
    ScheduledThreadPoolExecutor connectionThreadPool = new ScheduledThreadPoolExecutor(numThreads);
    for (int i = 0; i < numThreads; i++) {
      connectionThreadPool.scheduleAtFixedRate(() -> {
        try (Connection conn = timing("Getting connection from the connection pool", () -> dataSource.getConnection());
             PreparedStatement stmt = timing("Preparing statement", () -> conn.prepareStatement(repeatedQueryFinal))) {
          ResultSet rs = timing("Executing query", () -> stmt.executeQuery());
//          timing("fetch resultset metadata", () -> displayResultSetMetadata(rs.getMetaData()));
//          while (rs.next()) {
//            timing("fetch resultset row", () -> displayResultSetRow(rs));
//          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 0, queryFrequencySecs, TimeUnit.SECONDS);
    }
  }

  private static String ask(String prompt) throws IOException {
    System.out.println(prompt);
    System.out.print(">");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    return br.readLine();
  }

  private static <T> T timing(String label, Callable<T> callable) throws Exception {
    long start = System.currentTimeMillis();
    try {
      return callable.call();
    } finally {
      System.out.println(label + " took " + (System.currentTimeMillis() - start) + " ms");
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

  private static void printDataSourceStats(BasicDataSource bds) {
    try {
      Method getConnectionPoolMethod = BasicDataSource.class.getDeclaredMethod("getConnectionPool");
      getConnectionPoolMethod.setAccessible(true);

      GenericObjectPool<PoolableConnection> pool = (GenericObjectPool<PoolableConnection>) getConnectionPoolMethod.invoke(bds);

      System.out.println("Active\tIdle\tMax\tCreated\tBorrowed\tDstryBorrowed\tDstryEvicted\tDestroyed\t" + new java.util.Date());
      System.out.println(bds.getNumActive() + "\t" + bds.getNumIdle() + "\t" + bds.getMaxTotal() + "\t" + pool.getCreatedCount() + 
          "\t" + pool.getBorrowedCount() + "\t\t" + pool.getDestroyedByBorrowValidationCount() + 
          "\t\t" + pool.getDestroyedByEvictorCount() + "\t\t" + pool.getDestroyedCount());
    } catch (Exception ignored) {
    }
  }
}
