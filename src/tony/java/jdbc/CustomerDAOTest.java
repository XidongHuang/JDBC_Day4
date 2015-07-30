package tony.java.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;

import org.junit.Test;

public class CustomerDAOTest {

	CustomerDAO customerDAO = new CustomerDAO();
	
	@Test
	public void testBatch() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetForValue() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetForList() {
		fail("Not yet implemented");
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
		fail("Not yet implemented");
	}

}
