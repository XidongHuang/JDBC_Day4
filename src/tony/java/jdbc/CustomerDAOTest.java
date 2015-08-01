package tony.java.jdbc;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.sql.Connection;

import org.apache.commons.dbutils.QueryRunner;
import org.junit.Test;

import com.mysql.jdbc.PreparedStatement;

public class CustomerDAOTest {

	CustomerDAO customerDAO = new CustomerDAO();
	
	@Test
	public void testBatch() {
		
		
	}

	@Test
	public void testGetForValue() {
		
		Connection connection = null;
		
		try {
			connection = JDBCTools.getConnection();
			
			String sql = "SELECT name FROM customers WHERE id = ?";
			
			Object customer = customerDAO.getForValue(connection, sql, 4);
			System.out.println(customer);
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			JDBCTools.releaseDB(null, null, connection);
		}
		
		
	}

	@Test
	public void testGetForList() {
		Connection connection = null;
		
		
		
		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT * FROM customers WHERE id IN (?,?,?);"; 
			List<Map<String, Object>> customers =  customerDAO.getForList(connection, sql, 4,1,5);
			
			System.out.println(customers);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			JDBCTools.releaseDB(null, null, connection);
		}

		
		
	}

	@Test
	public void testGet() {
		Connection connection = null;
		
		
		try {
			connection = JDBCTools.getConnection();
			String sql = "SELECT id, name customerName, "
					+ "email, birth FROM customers WHERE id = ?";
			
			Customer cus = customerDAO.get(connection, sql, 5);
			System.out.println(cus);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally{
			JDBCTools.releaseDB(null, null, connection);
		}
		
		
		
	}

	@Test
	public void testUpdate() {
		Connection connection = null;
		
		try {
			connection = JDBCTools.getConnection();
			String sql = "UPDATE customers SET name = ? WHERE id = ?;";
			customerDAO.update(connection, sql, "NiuBi", 4);
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			
			JDBCTools.releaseDB(null, null, connection);
			
		}
		
		
	}

}
