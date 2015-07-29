package tony.java.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCTools {

	// 处理数据库事务
	//提交事务
	public static void commit(Connection connection) {

		if (connection != null) {
			try {

				connection.commit();

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

		}

	}

	//回滚事务
	public static void roolback(Connection connection) {

		if (connection != null) {
			try {

				connection.rollback();

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

		}

	}
	
	
	//上交事务
	public static void beginTx(Connection connection){
		if(connection != null){
			
			try {
				connection.setAutoCommit(false);
				
			} catch (Exception e) {
				e.printStackTrace();
				// TODO: handle exception
			}
			
		
		} 
		
		
	}
	
	

	/**
	 * ִ�� SQL ���, ʹ�� PreparedStatement
	 * 
	 * @param sql
	 * @param args:
	 *            ��д SQL ռλ��Ŀɱ����
	 */
	public static void update(String sql, Object... args) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;

		try {
			connection = JDBCTools.getConnection();
			preparedStatement = connection.prepareStatement(sql);

			for (int i = 0; i < args.length; i++) {
				preparedStatement.setObject(i + 1, args[i]);
			}

			preparedStatement.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			JDBCTools.releaseDB(null, preparedStatement, connection);
		}
	}

	/**
	 * ִ�� SQL �ķ���
	 * 
	 * @param sql:
	 *            insert, update �� delete�� ��� select
	 */
	public static void update(String sql) {
		Connection connection = null;
		Statement statement = null;

		try {
			// 1. ��ȡ��ݿ�����
			connection = getConnection();

			// 2. ���� Connection ����� createStatement() ������ȡ Statement ����
			statement = connection.createStatement();

			// 4. ���� SQL ���: ���� Statement ����� executeUpdate(sql) ����
			statement.executeUpdate(sql);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 5. �ر���ݿ���Դ: ��������ر�.
			releaseDB(null, statement, connection);
		}
	}

	/**
	 * �ͷ���ݿ���Դ�ķ���
	 * 
	 * @param resultSet
	 * @param statement
	 * @param connection
	 */
	public static void releaseDB(ResultSet resultSet, Statement statement, Connection connection) {

		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * ��ȡ��ݿ����ӵķ���
	 */
	public static Connection getConnection() throws IOException, ClassNotFoundException, SQLException {
		// 0. ��ȡ jdbc.properties
		/**
		 * 1). �����ļ���Ӧ Java �е� Properties �� 2). ����ʹ������������� bin
		 * Ŀ¼(��·����)���ļ�
		 */
		Properties properties = new Properties();
		InputStream inStream = JDBCTools.class.getClassLoader().getResourceAsStream("jdbc.properties");
		properties.load(inStream);

		// 1. ׼����ȡ���ӵ� 4 ���ַ�: user, password, jdbcUrl, driverClass
		String user = properties.getProperty("user");
		String password = properties.getProperty("password");
		String jdbcUrl = properties.getProperty("jdbcUrl");
		String driverClass = properties.getProperty("driver");

		// 2. ������: Class.forName(driverClass)
		Class.forName(driverClass);

		// 3. ����
		// DriverManager.getConnection(jdbcUrl, user, password)
		// ��ȡ��ݿ�����
		Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
		return connection;
	}

}
