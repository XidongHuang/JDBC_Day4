package tony.java.jdbc;

import java.lang.Thread;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.junit.Test;
import org.omg.CORBA.Current;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.interceptors.ServerStatusDiffInterceptor;

import java.sql.PreparedStatement;

import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

public class JDBCTest {
	static int count = 0;
	
	@Test
	public void testJDBCTools() throws ClassNotFoundException, IOException, SQLException{
		Connection connection = JDBCTools.getConnection();
		
		System.out.println(connection);
		
	}
	
	

	/**
	 * 1. 创建c3p0-config.xml 文件，参考帮助文档中Appendix B：Configuation F
	 * 2. 创建 ComboPooledDataSource 实例:
	 * DataSource dataSource = new ComboPooledDataSource("helloc3p0");
	 * 3. 从DataSource 实例中获取数据库链接
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testC3P0WithConfigFile() throws SQLException{
		DataSource dataSource = new ComboPooledDataSource("helloc3p0");
		System.out.println(dataSource.getConnection());
		
		ComboPooledDataSource comboPooledDataSource = (ComboPooledDataSource) dataSource;
		System.out.println(comboPooledDataSource.getMaxStatements());
	
	}
	
	
	
	
	/**
	 * @throws PropertyVetoException
	 * @throws SQLException 
	 * 
	 * 
	 */
	@Test
	public void testC3P0() throws PropertyVetoException, SQLException {

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setDriverClass("com.mysql.jdbc.Driver"); // loads the jdbc driver
		cpds.setJdbcUrl("jdbc:mysql://testdb.c9xqmbjdijez.us-west-2.rds.amazonaws.com:3306/testDB");
		cpds.setUser("root");
		cpds.setPassword("12345678");
		
		System.out.println(cpds.getConnection());
		
	

	}

	/**
	 * 1. 加载dbcp 的properties 配置文件: 配置文件中的键需要来自BasicDataSource的属性 2. 调用
	 * BasicDataSourceFactory 的createDataSrouce 方法创建DataSourceFactory 的实例 3.
	 * 从DataSource 实例中获取数据库链接
	 * 
	 * @throws Exception
	 */

	@Test
	public void testDBCPWithDataSourceFactory() throws Exception {

		Properties properties = new Properties();
		InputStream inStream = JDBCTest.class.getClassLoader().getResourceAsStream("dbcp.properties");
		properties.load(inStream);

		DataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
		System.out.println(dataSource.getConnection());

		BasicDataSource basicDataSource = (BasicDataSource) dataSource;
		System.out.println(basicDataSource.getMaxWait());

	}

	/**
	 * 使用DBCP 数据库连接池 1. 加入jar 包 pool-15.5, DBCP 1.4 2. 创建数据库连接池 3. 创建 DBCP 数据源实例
	 * 4. 从数据源中获取数据库链接
	 * 
	 * @throws SQLException
	 */

	@Test
	public void testDBCP() throws SQLException {

		// 创建 DBCP 数据源实例

		final BasicDataSource dataSource = new BasicDataSource();

		// 2. 为数据源实例制定必须的属性
		dataSource.setUsername("root");
		dataSource.setPassword("12345678");
		dataSource.setUrl("jdbc:mysql://testdb.c9xqmbjdijez.us-west-2.rds.amazonaws.com:3306/testDB");
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");

		// 3. 指定数据源的一些可选的属性
		// 3.1 指定数据库连接池中初始化连接数的个数
		dataSource.setInitialSize(5);

		// 3.2 指定最大的连接数: 同意时刻可以同时向数据库申请的链接数
		dataSource.setMaxActive(5);

		// 3.3 指定最小连接数: 在数据库连接池空闲状态下，连接池中最少有多少个链接
		dataSource.setMinIdle(2);

		// 3.4 等待数据库连接池分配链接的最长时间，单位为毫秒。超出该时间将抛出异常
		dataSource.setMaxWait(1000 * 5);

		// 4. 从数据源中获取数据库链接
		Connection connection = dataSource.getConnection();
		System.out.println(connection.getClass());

		connection = dataSource.getConnection();
		System.out.println(connection.getClass());

		connection = dataSource.getConnection();
		System.out.println(connection.getClass());

		connection = dataSource.getConnection();
		System.out.println(connection.getClass());

		Connection connection2 = dataSource.getConnection();
		System.out.println(">" + connection.getClass());

		new Thread() {
			public void run() {
				try {
					Connection conn = dataSource.getConnection();
					System.out.println(conn.getClass());
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};

		}.start();

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		connection2.close();
	}

	@Test
	public void testBatch() {

		Connection connection = null;
		PreparedStatement preparedStatement = null;
		String sql = null;

		try {

			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);

			sql = "INSERT INTO customers VALUES(?,?,?)";
			preparedStatement = connection.prepareStatement(sql);

			long begin = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				preparedStatement.setInt(1, i + 1);
				preparedStatement.setString(2, "name_" + i);
				preparedStatement.setString(3, "location" + i);
				count++;
				System.out.println(count);
				// "积攒" SQL
				preparedStatement.addBatch();

				// 当"积攒" 到一定程度，就统一的执行一次. 并且清空先前"积攒" 的SQL
				if ((i + 1) % 50000 == 0) {
					preparedStatement.executeBatch();
					preparedStatement.clearBatch();
				}

			}

			// 若总条数不是批量数值的整数倍，则还需要在额外地执行一次
			if (10000 % 300 != 0) {
				preparedStatement.executeBatch();
				preparedStatement.clearBatch();
			}

			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));// 159898
			JDBCTools.commit(connection);
		} catch (Exception e) {
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, preparedStatement, connection);

		}

	}

	@Test
	public void testBatchWithPreparedStatement() {
		// static int count = 0;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		String sql = null;

		try {

			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);
			sql = "INSERT INTO customers VALUES(?,?,?)";
			preparedStatement = connection.prepareStatement(sql);

			long begin = System.currentTimeMillis();
			for (int i = 0; i < 1000; i++) {
				preparedStatement.setInt(1, i + 1);
				preparedStatement.setString(2, "name_" + i);
				preparedStatement.setString(3, "location" + i);
				System.out.println(sql);
				preparedStatement.executeUpdate();
			}
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));// 159898
			JDBCTools.commit(connection);
		} catch (Exception e) {
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, preparedStatement, connection);

		}

	}

	/**
	 * 向Oracle 的Customer 数据表中插入10万条记录 测试如何插入用时最短 1. 使用Statement
	 */

	@Test
	public void testBatchWithStatement() {

		Connection connection = null;
		Statement statement = null;
		String sql = null;

		try {

			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);
			statement = connection.createStatement();

			long begin = System.currentTimeMillis();
			for (int i = 0; i < 1000; i++) {
				sql = "INSERT INTO customers VALUES(" + (i + 1) + ",'name_" + i + "','location_" + i + "');";
				System.out.println(sql);
				statement.executeUpdate(sql);

			}
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));// 79257
			JDBCTools.commit(connection);
		} catch (Exception e) {
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, statement, connection);

		}

	}

}
