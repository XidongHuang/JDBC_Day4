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

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class JDBCTools {

	// 处理数据库事务
	// 提交事务
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

	// 回滚事务
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

	// 上交事务
	public static void beginTx(Connection connection) {
		if (connection != null) {

			try {
				connection.setAutoCommit(false);

			} catch (Exception e) {
				e.printStackTrace();
				// TODO: handle exception
			}

		}

	}

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

	public static void update(String sql) {
		Connection connection = null;
		Statement statement = null;

		try {
			connection = getConnection();

			statement = connection.createStatement();

			statement.executeUpdate(sql);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			releaseDB(null, statement, connection);
		}
	}

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
				//数据库连接池的Connection 对象进行 close时
				//并不是真的进行关闭，而是把该数据库链接会归还到数据库连接池中
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	private static DataSource dataSource = null;
	
	//数据库连接池应该只被初始化一次
	static {
		dataSource = new ComboPooledDataSource("helloc3p0");
	}
	
	
	public static Connection getConnection() throws IOException, ClassNotFoundException, SQLException {

		return dataSource.getConnection();
	}

}
