package tony.java.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import com.mysql.jdbc.PreparedStatement;




/**
 * 使用 QueryRunner 提供其具体的实现
 * 
 * @author tony
 *
 * @param <T>
 */

public class JdbcDaoImp<T> implements DAO<T> {

	private QueryRunner queryRunner = null; 
	private Class<T> type;
	
	public JdbcDaoImp() {

		queryRunner = new QueryRunner();
		type = ReflectionUtil.getSuperGenericType(getClass());
	}
	
	@Override
	public void batch(Connection connection, String sql, Object[]... args) {
	
		try {
			queryRunner.batch(connection,sql, args);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public <E> E getForValue(Connection connection, String sql, Object... args) throws SQLException {

		
		return (E) queryRunner.query(connection, sql, new ScalarHandler(),args);
	}

	@Override
	public List<Map<String, Object>> getForList(Connection connection, String sql, Object... args) throws SQLException {

		
		
		return queryRunner.query(connection, sql, new MapListHandler(),args);
	}

	@Override
	public T get(Connection connection, String sql, Object... args) throws SQLException {

		
		
		return queryRunner.query(connection, sql, 
				new BeanHandler<>(type),args);
	}

	@Override
	public void update(Connection connection, String sql, Object... args) throws SQLException {
		// TODO Auto-generated method stub
		queryRunner.update(connection, sql,args);
	}

}
