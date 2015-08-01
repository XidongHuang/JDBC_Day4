package tony.java.jdbc;

import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 访问数据的DAO 接口，
 * 里边定义好访问数据表的各种方法 
 * 
 * @author tony
 * @param T: DAO 处理的实体类的类型
 */
public interface DAO<T> {

	/**
	 * 批量处理的方法
	 * @param connection
	 * @param sql
	 * @param args: 填充站位符 Object[] 类型的可变参数
	 */
	void batch(Connection connection,
			String sql,Object []... args);
	
	
	/**
	 * 返回具体一个值，例如总人数，平均工资，某一个人的email等
	 * 
	 * @param connection
	 * @param sql
	 * @param args
	 * @return
	 * @throws SQLException 
	 */
	<E> E getForValue(Connection connection, String sql,
			Object ... args) throws SQLException;
	
	
	/**
	 * 返回T 的一个集合
	 * @param connection
	 * @param sql
	 * @param args
	 * @return
	 * @throws SQLException 
	 */
	List<Map<String, Object>> getForList(Connection connection, String sql,
			Object ... args) throws SQLException;
	
	/**
	 * 返回一个 T 的对象
	 * @param connection
	 * @param sql
	 * @param args
	 * @return
	 * @throws SQLException 
	 */
	T get(Connection connection, String sql,
			Object ... args) throws SQLException;
	
	/**
	 * INSERT, UPDATE, DELETE
	 * 
	 * @param connection
	 * @param sql
	 * @param args
	 * @throws SQLException 
	 */
	
	void update(Connection connection, String sql,
			Object ... args) throws Exception;
	
	
	
	
	
}
