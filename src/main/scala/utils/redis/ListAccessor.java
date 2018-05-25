package utils.redis;

import org.springframework.data.redis.core.ListOperations;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <P>List数据类型访问器</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @since 1.0.0
 */
@SuppressWarnings("unchecked")
public final class ListAccessor<K,V> 
{
	private ListOperations<K,V> ops = null;
	
	ListAccessor(ListOperations<K,V> listops) {
		this.ops = listops;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 通过索引号获取列表中的一个元素。
	 * 也可以使用负数下标，-1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * 若指定的索引值不在列表区间范围内，返回NULL。
	 * </PRE>
	 * @param key
	 * @param index
	 * @return 
	 *********************************************************************************/
	public V index(K key, long index) {
		return ops.index(key, index);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除并获取列表的第一个元素。
	 * 若 key 不存在, 返回 NULL。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public V lpop(K key) {
		return ops.leftPop(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移出并获取列表的第一个元素。
	 * 若列表没有元素, 会阻塞列表, 直到等待超时或发现可移除元素为止。
	 * 若 key 不存在, 返回 NULL。
	 * </PRE>
	 * @param key
	 * @param timeout
	 * @param unit
	 * @return 
	 *********************************************************************************/
	public V lpop(K key, long timeout, TimeUnit unit) {
		return ops.leftPop(key, timeout, unit);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个元素插入到列表头部。
	 * 若 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 列表的长度
	 *********************************************************************************/
	public Long lpush(K key, V value) {
		return ops.leftPush(key, value);
	}
	
	/**********************************************************************************
	 * <P></P>
	 * @param key
	 * @param pivot
	 * @param value
	 * @return 
	 *********************************************************************************/
	public Long lpush(K key, V pivot, V value) {
		return ops.leftPush(key, pivot, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将多个元素插入到列表头部。
	 * 若 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 列表的长度
	 *********************************************************************************/
	public Long lpushm(K key, Collection<V> values) {
		return ops.leftPushAll(key, values);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将多个元素插入到列表头部。
	 * 若 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 列表的长度
	 *********************************************************************************/
	public Long lpushm(K key, V... values) {
		return ops.leftPushAll(key, values);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个元素插入到已存在的列表头部。
	 * 若 key 不存在, 操作被忽略。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 列表的长度
	 *********************************************************************************/
	public Long lpushx(K key, V value) {
		return ops.leftPushIfPresent(key, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 返回列表中指定区间内的元素。
	 * 区间以偏移量 START 和 END 指定。 
	 * 其中 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推。 
	 * 也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end
	 * @return 
	 *********************************************************************************/
	public List<V> range(K key, long start, long end) {
		return ops.range(key, end, end);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 从移除列表中移除与VALUE相等的COUNT数量的元素。
	 * 
	 * COUNT 的值可以是以下几种：
	 * count 大于 0 : 从列表头部向尾部搜索，移除与VALUE相等的元素，数量为COUNT。
	 * count 小于 0 : 从列表尾部向头部搜索，移除与VALUE相等的元素，数量为COUNT。
	 * count 等于 0 : 移除表中所有与VALUE相等的值。
	 * </PRE>
	 * @param key
	 * @param count
	 * @param value
	 * @return 被移除元素的数量。列表不存在时, 返回0。
	 *********************************************************************************/
	public Long remove(K key, long count, Object value) {
		return ops.remove(key, count, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除并获取列表的最后一个元素。
	 * 若 key 不存在, 返回 NULL。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public V rpop(K key) {
		return ops.leftPop(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移出并获取列表的最后一个元素。
	 * 若列表没有元素, 会阻塞列表, 直到等待超时或发现可移除元素为止。
	 * 若 key 不存在, 返回 NULL。
	 * </PRE>
	 * @param key
	 * @param timeout
	 * @param unit
	 * @return 
	 *********************************************************************************/
	public V rpop(K key, long timeout, TimeUnit unit) {
		return ops.leftPop(key, timeout, unit);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 从源列表尾部移除一个元素，将移除的元素插入到目标列表头部，并返回。
	 * 若 sourceKey 或 destinationKey 不存在, 返回NULL。
	 * </PRE>
	 * @param sourceKey
	 * @param destinationKey
	 * @return 
	 *********************************************************************************/
	public V rpoplpush(K sourceKey, K destinationKey) {
		return ops.rightPopAndLeftPush(sourceKey, destinationKey);
	} 
	
	/**********************************************************************************
	 * <PRE>
	 * 从源列表尾部移除一个元素，将移除的元素插入到目标列表头部，并返回。
	 * 若源列表没有元素，会阻塞源列表，直到超时或发现可移除的元素为止。
	 * 若超时，返回NULL。
	 * 若 sourceKey 或 destinationKey 不存在, 返回NULL。
	 * </PRE>
	 * @param sourceKey
	 * @param destinationKey
	 * @param timeout
	 * @param unit
	 * @return 
	 *********************************************************************************/
	public V rpoplpush(K sourceKey, K destinationKey, long timeout, TimeUnit unit) {
		return ops.rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个元素插入到列表尾部。
	 * 若 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 列表的长度
	 *********************************************************************************/
	public Long rpush(K key, V value) {
		return ops.rightPush(key, value);
	}
	
	/**********************************************************************************
	 * <P></P>
	 * @param key
	 * @param pivot
	 * @param value
	 * @return 
	 *********************************************************************************/
	public Long rpush(K key, V pivot, V value) {
		return ops.rightPush(key, pivot, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将多个元素插入到列表尾部。
	 * 若 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 列表的长度
	 *********************************************************************************/
	public Long rpushm(K key, Collection<V> values) {
		return ops.rightPushAll(key, values);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将多个元素插入到列表尾部。
	 * 若 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 列表的长度
	 *********************************************************************************/
	public Long rpushm(K key, V... values) {
		return ops.rightPushAll(key, values);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个元素插入到已存在的列表尾部。
	 * 若 key 不存在, 操作被忽略。
	 * 若 key 存在, 但不是列表类型时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param value
	 * @return 列表的长度
	 *********************************************************************************/
	public Long rpushx(K key, V value) {
		return ops.rightPushIfPresent(key, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 设置指定索引号位置的元素值。
	 * 当索引号超出范围，或对一个空列表进行 LSET 时，返回一个错误。
	 * </PRE>
	 * @param key
	 * @param index
	 * @param value 
	 *********************************************************************************/
	public void set(K key, long index, V value) {
		ops.set(key, index, value);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取列表的长度。
	 * 若 key 不存在，则 key 被解释为一个空列表，返回 0 。 
	 * 若 key 不是列表类型，返回错误码 -1L 。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public Long size(K key) {
		try {
			return ops.size(key);
		} catch(Exception e) {
			return -1L;
		}
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 对列表进行剪裁，让列表只保留指定区间内的元素，不在指定区间内的元素都将被删除。
	 * 下标 0 表示列表的第一个元素，1 表示列表的第二个元素，以此类推。
	 * 也可以使用负数下标，-1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end 
	 *********************************************************************************/
	public void trim(K key, long start, long end) {
		ops.trim(key, start, end);
	}
}
