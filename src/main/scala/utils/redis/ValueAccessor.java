package utils.redis;

import org.springframework.data.redis.core.ValueOperations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <P>Value类型访问器</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @since 1.0.0
 */
public final class ValueAccessor<K,V> {
	
	private ValueOperations<K,V> ops;
	
	ValueAccessor(ValueOperations<K,V> valueops) {
		this.ops = valueops;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 为指定的 key 追加值。
	 * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。
	 * 如果 key 不存在， APPEND 就简单地将给定 key 设为 value 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 追加指定值之后 key 中字符串的长度
	 *********************************************************************************/
	public Integer append(K key, String value) {
		return ops.append(key, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取指定 key 中字符串的子字符串。
	 * 字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)。
	 * 如果 key 不存在，返回 NULL 。
	 * 如果 key 储存的值不是字符串类型，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end
	 * @return 截取的子字符串
	 *********************************************************************************/
	public String get(K key, long start, long end) {
		try {
			return ops.get(key, start, end);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取指定 key 的值。
	 * 如果 key 不存在，返回 NULL 。
	 * 如果 key 储存的值不是字符串类型，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public V get(K key) {
		try {
			return ops.get(key);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 设置指定 key 的值，并返回 key 的旧值。
	 * 当 key 没有旧值时，即 key 不存在时，返回 NULL 。
	 * 当 key 存在但不是字符串类型时，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 
	 *********************************************************************************/
	public V getset(K key, V value) {
		try {
			return ops.getAndSet(key, value);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取 key 所储存的字符串值中指定偏移量上的位(bit)。
	 * 当偏移量 OFFSET 比字符串值的长度大，或者 key 不存在时，返回 FALSE 。
	 * </PRE>
	 * @param key
	 * @param offset
	 * @return 字符串值指定偏移量上的位(bit)
	 *********************************************************************************/
	public Boolean getbit(K key, long offset) {
		return ops.getBit(key, offset);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 为 key 中所储存的值加上指定的浮点数增量值。
	 * 如果 key 不存在，先将 key 的值设为 0 ，再执行加法操作。
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，则返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param delta
	 * @return 执行命令之后key的值
	 *********************************************************************************/
	public Double increment(K key, double delta) {
		return ops.increment(key, delta);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将 key 中储存的数字加上指定的增量值。
	 * 如果 key 不存在，那么 key 的值先被初始化为 0 ，再执行增量操作。
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，则返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param delta
	 * @return 执行命令之后key的值
	 *********************************************************************************/
	public Long increment(K key, long delta) {
		return ops.increment(key, delta);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取一个或多个给定 key 的值。 
	 * 如果给定的 key 里面，有某个 key 不存在，那么与这个 key 对应的值为 NULL 。
	 * </PRE>
	 * @param keys
	 * @return 包含所有给定 key 的值的列表
	 *********************************************************************************/
	public List<V> mget(Collection<K> keys) {
		try {
			return ops.multiGet(keys);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 同时设置一个或多个 key-value 对。
	 * </PRE>
	 * @param m 
	 *********************************************************************************/
	public void	mset(Map<? extends K,? extends V> m) {
		ops.multiSet(m);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 仅当所有给定 key 都不存在时，同时设置一个或多个 key-value 对。
	 * 当所有 key 都成功设置，返回 TRUE 。 
	 * 如果有至少一个 key 设置失败，或至少有一个 key 已经存在，则返回 FALSE 。
	 * </PRE>
	 * @param m
	 * @return 
	 *********************************************************************************/
	public Boolean msetnx(Map<? extends K,? extends V> m) {
		return ops.multiSetIfAbsent(m);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 设置给定 key 的值。
	 * 如果 key 已经存储其他值，覆写旧值，且无视类型。
	 * </PRE>
	 * @param key
	 * @param value 
	 *********************************************************************************/
	public void set(K key, V value) {
		ops.set(key, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 用指定的字符串覆盖给定 key 所储存的字符串值，覆盖的位置从偏移量 offset 开始。
	 * </PRE>
	 * @param key
	 * @param value
	 * @param offset 
	 *********************************************************************************/
	public void set(K key, V value, long offset) {
		ops.set(key, value, offset);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 设置 key 的值的同时设置生存时间。
	 * </PRE>
	 * @param key 
	 * @param value 
	 * @param timeout 
	 * @param unit 
	 *********************************************************************************/
	public void set(K key, V value, long timeout, TimeUnit unit) {
		ops.set(key, value, timeout, unit);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
	 * </PRE>
	 * @param key
	 * @param offset
	 * @param value
	 * @return 指定偏移量位置原来储存的位
	 *********************************************************************************/
	public Boolean setbit(K key, long offset, boolean value) {
		return ops.setBit(key, offset, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 在指定的 key 不存在时，为 key 设置指定的值。
	 * 设置成功，返回 TRUE 。
	 * 设置失败，返回 FALSE 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 
	 *********************************************************************************/
	public Boolean setnx(K key, V value) {
		return ops.setIfAbsent(key, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取指定 key 所储存的字符串值的长度。
	 * 当 key 储存的不是字符串值时，返回 -1L 。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public Long size(K key) {
		return ops.size(key);
	}
}
