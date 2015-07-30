package tony.java.jdbc;



import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * ·ŽÉäµÄ Utils º¯ÊýŒ¯ºÏ
 * Ìá¹©·ÃÎÊËœÓÐ±äÁ¿, »ñÈ¡·ºÐÍÀàÐÍ Class, ÌáÈ¡Œ¯ºÏÖÐÔªËØÊôÐÔµÈ Utils º¯Êý
 * @author Administrator
 *
 */
public class ReflectionUtil {

	
	/**
	 * Íš¹ý·ŽÉä, »ñµÃ¶šÒå Class Ê±ÉùÃ÷µÄžžÀàµÄ·ºÐÍ²ÎÊýµÄÀàÐÍ
	 * Èç: public EmployeeDao extends BaseDao<Employee, String>
	 * @param clazz
	 * @param index
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Class getSuperClassGenricType(Class clazz, int index){
		Type genType = clazz.getGenericSuperclass();
		
		if(!(genType instanceof ParameterizedType)){
			return Object.class;
		}
		
		Type [] params = ((ParameterizedType)genType).getActualTypeArguments();
		
		if(index >= params.length || index < 0){
			return Object.class;
		}
		
		if(!(params[index] instanceof Class)){
			return Object.class;
		}
		
		return (Class) params[index];
	}
	
	/**
	 * Íš¹ý·ŽÉä, »ñµÃ Class ¶šÒåÖÐÉùÃ÷µÄžžÀàµÄ·ºÐÍ²ÎÊýÀàÐÍ
	 * Èç: public EmployeeDao extends BaseDao<Employee, String>
	 * @param <T>
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static<T> Class<T> getSuperGenericType(Class clazz){
		return getSuperClassGenricType(clazz, 0);
	}
	
	/**
	 * Ñ­»·ÏòÉÏ×ªÐÍ, »ñÈ¡¶ÔÏóµÄ DeclaredMethod
	 * @param object
	 * @param methodName
	 * @param parameterTypes
	 * @return
	 */
	public static Method getDeclaredMethod(Object object, String methodName, Class<?>[] parameterTypes){
		
		for(Class<?> superClass = object.getClass(); superClass != Object.class; superClass = superClass.getSuperclass()){
			try {
				//superClass.getMethod(methodName, parameterTypes);
				return superClass.getDeclaredMethod(methodName, parameterTypes);
			} catch (NoSuchMethodException e) {
				//Method ²»ÔÚµ±Ç°Àà¶šÒå, ŒÌÐøÏòÉÏ×ªÐÍ
			}
			//..
		}
		
		return null;
	}
	
	/**
	 * Ê¹ filed ±äÎª¿É·ÃÎÊ
	 * @param field
	 */
	public static void makeAccessible(Field field){
		if(!Modifier.isPublic(field.getModifiers())){
			field.setAccessible(true);
		}
	}
	
	/**
	 * Ñ­»·ÏòÉÏ×ªÐÍ, »ñÈ¡¶ÔÏóµÄ DeclaredField
	 * @param object
	 * @param filedName
	 * @return
	 */
	public static Field getDeclaredField(Object object, String filedName){
		
		for(Class<?> superClass = object.getClass(); superClass != Object.class; superClass = superClass.getSuperclass()){
			try {
				return superClass.getDeclaredField(filedName);
			} catch (NoSuchFieldException e) {
				//Field ²»ÔÚµ±Ç°Àà¶šÒå, ŒÌÐøÏòÉÏ×ªÐÍ
			}
		}
		return null;
	}
	
	/**
	 * Ö±œÓµ÷ÓÃ¶ÔÏó·œ·š, ¶øºöÂÔÐÞÊÎ·û(private, protected)
	 * @param object
	 * @param methodName
	 * @param parameterTypes
	 * @param parameters
	 * @return
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 */
	public static Object invokeMethod(Object object, String methodName, Class<?> [] parameterTypes,
			Object [] parameters) throws InvocationTargetException{
		
		Method method = getDeclaredMethod(object, methodName, parameterTypes);
		
		if(method == null){
			throw new IllegalArgumentException("Could not find method [" + methodName + "] on target [" + object + "]");
		}
		
		method.setAccessible(true);
		
		try {
			return method.invoke(object, parameters);
		} catch(IllegalAccessException e) {
			System.out.println("²»¿ÉÄÜÅ×³öµÄÒì³£");
		} 
		
		return null;
	}
	
	/**
	 * Ö±œÓÉèÖÃ¶ÔÏóÊôÐÔÖµ, ºöÂÔ private/protected ÐÞÊÎ·û, Ò²²»Ÿ­¹ý setter
	 * @param object
	 * @param fieldName
	 * @param value
	 */
	public static void setFieldValue(Object object, String fieldName, Object value){
		Field field = getDeclaredField(object, fieldName);
		
		if (field == null)
			throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + object + "]");
		
		makeAccessible(field);
		
		try {
			field.set(object, value);
		} catch (IllegalAccessException e) {
			System.out.println("²»¿ÉÄÜÅ×³öµÄÒì³£");
		}
	}
	
	/**
	 * Ö±œÓ¶ÁÈ¡¶ÔÏóµÄÊôÐÔÖµ, ºöÂÔ private/protected ÐÞÊÎ·û, Ò²²»Ÿ­¹ý getter
	 * @param object
	 * @param fieldName
	 * @return
	 */
	public static Object getFieldValue(Object object, String fieldName){
		Field field = getDeclaredField(object, fieldName);
		
		if (field == null)
			throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + object + "]");
		
		makeAccessible(field);
		
		Object result = null;
		
		try {
			result = field.get(object);
		} catch (IllegalAccessException e) {
			System.out.println("²»¿ÉÄÜÅ×³öµÄÒì³£");
		}
		
		return result;
	}
}