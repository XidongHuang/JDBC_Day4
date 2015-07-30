package tony.java.jdbc;

import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Test;

/**
 * 测试DBUtils 工具类
 * 
 * @author tony
 *
 */
public class DBUtilsTest {

	/**
	 * ScalarHandler： 把结果结转为一个数值(可以是任意基本7数据类型和字符串，Date 等)
	 * 
	 */
	@Test
	public void testScalarHandler(){
		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT name CustomerName " +
			"FROM customers WHERE id = ?";

			Object customer = queryRunner.query(connection,
					sql, new ScalarHandler(),5);
			
			System.out.println(customer);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);
		}
		
		
	}
	
	
	
	/**
	 * MapListHandler： 将结果集转为一个Map 的List
	 * Map 对应查询的一条记录: 键: SQL 查询的列名(不是列的别名)，值: 列的值
	 * 而 MapListHandler: 返回的是多条记录对应的Map 的集合
	 * 
	 */
	@Test
	public void testMapListHandler(){
		
		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id,name customerName,email,birth " +
			"FROM customers";

			List<Map<String, Object>> customer = queryRunner.query(connection,
					sql, new MapListHandler());
			System.out.println(customer);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);
		}
		
		
		
		
	}
	
	
	
	/**
	 * MapHandler: 返回SQL 对应的第一条记录对应的Map 对象
	 * 键值对: 键: SQL 查询的列名(不是列的别名)，值: 列的值
	 * 
	 */
	@Test
	public void testMapHandler(){
		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id,name customerName,email,birth " +
			"FROM customers";

			Map<String, Object> customer = queryRunner.query(connection,
					sql, new MapHandler());
			System.out.println(customer);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);
		}

		
		
		
	}
	
	
	
	
	
	/**
	 * BeanListHandler: 把结果集转为一个 List, 该List不为 null，
	 * 但可能为空集合(size() 方法返回0) 
	 * 若SQL 语句的确能够查询到记录，List 中存放创建 BeanListHandler 传入
	 * 的Class 对象对应的对象
	 */
	@Test
	public void testBeanListHandler() {

		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id,name,email,birth " +
			"FROM customers";

			Customer customer = queryRunner.query(connection, sql, new BeanHandler(Customer.class));
			System.out.println(customer);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);
		}

	}

	/**
	 * BeanHandler: 把结果集的第一条记录转为创建 BeanHandler 对象时传入的Class 参数对应的对象
	 * 
	 */
	@Test
	public void testBeanHanlder() {
		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id,name,email,birth " + "FROM customers WHERE id = ?";

			Customer customer = queryRunner.query(connection, sql, new BeanHandler(Customer.class), 5);
			System.out.println(customer);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);
		}

	}

	QueryRunner queryRunner = new QueryRunner();

	class MyResultSetHandler implements ResultSetHandler {

		@Override
		public Object handle(ResultSet resultSet) throws SQLException {
			// System.out.println("handle...");
			// return "testDB";

			List<Customer> customers = new ArrayList<>();

			while (resultSet.next()) {
				Integer id = resultSet.getInt(1);
				String name = resultSet.getString(2);
				String email = resultSet.getString(3);
				Date birth = resultSet.getDate(4);

				Customer customer = new Customer(id, name, email, birth);

				customers.add(customer);
			}

			return customers;
		}

	}

	/**
	 * QueryRunner 的 query 方法的返回值取决于其ResultSetHandler 参数的 handle 方法的返回值
	 * 
	 */

	@Test
	public void testQuery() {
		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id,name,email,birth " + "FROM customers";

			Object obj = queryRunner.query(connection, sql, new MyResultSetHandler());

			System.out.println(obj);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			JDBCTools.releaseDB(null, null, connection);

		}

	}

	/**
	 * 测试QueryRunner 类的update 方法
	 * 
	 */
	@Test
	public void testQueryRunnerUpdate() {
		// 1. 创建QueryRunner 的实现类
		QueryRunner queryRunner = new QueryRunner();

		// 2. 使用其update方法
		String sql = "DELETE FROM customers " + "WHERE id IN(?,?)";

		Connection connection = null;

		try {
			connection = JDBCTools.getConnection();
			queryRunner.update(connection, sql, 3, 4);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			JDBCTools.releaseDB(null, null, connection);
		}

	}
}
