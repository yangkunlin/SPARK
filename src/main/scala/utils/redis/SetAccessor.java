package utils.redis;

import org.springframework.data.redis.core.SetOperations;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * <P>Set类型访问器</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @since 1.0.0
 */
public final class SetAccessor<K,V> {
	
	private SetOperations<K,V> ops = null;
	
	SetAccessor(SetOperations<K,V> setops) {
		this.ops = setops;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个或多个成员元素加入到集合中。
	 * 已经存在于集合的成员元素将被忽略。
	 * 若集合 key 不存在，则新创建一个集合，再执行添加操作。
	 * 当集合 key 不是集合类型时，返回 -1 。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 被添加到集合中的新元素的数量
	 *********************************************************************************/
	@SuppressWarnings("unchecked")
	public Long add(K key, V... values) {
		try {
			return ops.add(key, values);
		} catch(Exception e) {
		}
		return -1L;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获得指定集合之间的差集。
	 * 不存在的集合 key 将视为空集。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @return 包含差集成员的列表
	 *********************************************************************************/
	public Set<V> difference(K key, Collection<K> otherKeys) {
		return ops.difference(key, otherKeys);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code difference(K key, Collection<K> otherKeys)} 方法。
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @return 
	 *********************************************************************************/
	public Set<V> difference(K key, K otherKey) {
		return ops.difference(key, otherKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将指定集合之间的差集存储在目标集合中。
	 * 如果目标集合 key 已存在，则会被覆盖。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @param destinationKey
	 * @return 结果集中的元素数量
	 *********************************************************************************/
	public Long differenceStore(K key, Collection<K> otherKeys, K destinationKey) {
		return ops.differenceAndStore(key, otherKeys, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code diffstore(K key, Collection<K> otherKeys, K destinationKey) } 方法
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @param destinationKey
	 * @return 结果集中的元素数量
	 *********************************************************************************/
	public Long differenceStore(K key, K otherKey, K destinationKey) {
		return ops.differenceAndStore(key, otherKey, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获得所有指定集合的交集。
	 * 不存在的集合 key 被视为空集。 
	 * 当指定集合中有一个空集时，结果也为空集(根据集合运算定律)。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @return 交集成员的列表
	 *********************************************************************************/
	public Set<V> intersect(K key, Collection<K> otherKeys) {
		return ops.intersect(key, otherKeys);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code intersect(K key, Collection<K> otherKeys) } 方法。
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @return 交集成员的列表
	 *********************************************************************************/
	public Set<V> intersect(K key, K otherKey) {
		return ops.intersect(key, otherKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将指定集合之间的交集存储在目标集合中。
	 * 如果目标集合已存在，则将其覆盖。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @param destinationKey
	 * @return 交集成员的列表
	 *********************************************************************************/
	public Long intersectStore(K key, Collection<K> otherKeys, K destinationKey) {
		return ops.intersectAndStore(key, otherKeys, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code intersectStore(K key, Collection<K> otherKeys, K destinationKey)} 方法
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @param destinationKey
	 * @return 交集成员的列表
	 *********************************************************************************/
	public Long intersectStore(K key, K otherKey, K destinationKey) {
		return ops.intersectAndStore(key, otherKey, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 判断元素是否是集合的成员。
	 * 若元素是集合的成员，返回 TRUE 。 
	 * 若元素不是集合的成员，或 key 不存在，返回 FALSE 。
	 * </PRE>
	 * @param key
	 * @param o
	 * @return 
	 *********************************************************************************/
	public Boolean isMember(K key, Object o) {
		return ops.isMember(key, o);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取集合中的所有的成员。
	 * 不存在的集合 key 被视为空集合。
	 * </PRE>
	 * @param key
	 * @return 集合中的所有成员
	 *********************************************************************************/
	public Set<V> members(K key) {
		return ops.members(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将指定成员元素从源集合移动到目标集合。
	 * 此命令是原子操作。
	 * 若源集合不存在或不包含指定的元素，则此命令不执行任何操作，仅返回 FALSE 。
	 * 否则，元素从源集合中被移除，并添加到目标集合中。
	 * 当目标集合已经包含指定元素时，此命令只是简单地将元素从源集合中删除。
	 * 操作失败时，返回 FALSE 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @param destinationKey
	 * @return 
	 *********************************************************************************/
	public Boolean move(K key, V value, K destinationKey) {
		return ops.move(key, value, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除并返回集合中的一个随机元素。
	 * 当集合不存在或是空集时，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @return 被移除的随机元素
	 *********************************************************************************/
	public V pop(K key) {
		return ops.pop(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取集合中的一个随机元素。
	 * 若 key 存在或集合为空，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public V randomMember(K key) {
		return ops.randomMember(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取集合中的一个随机元素。
	 * 如果 count 为正数，且小于集合基数，则返回一个包含 count 个元素的数组，且数组中的元素各不相同。
	 * 如果 count 大于等于集合基数，那么返回整个集合。
	 * 如果 count 为负数，则返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为count的绝对值。
	 * 若 key 存在或集合为空，返回空列表。
	 * </PRE>
	 * @param key
	 * @param count
	 * @return 
	 *********************************************************************************/
	public List<V> randomMembers(K key, long count) {
		return ops.randomMembers(key, count);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除集合中的一个或多个成员元素，不存在的成员元素会被忽略。
	 * 当操作失败时，返回 -1L 。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 被成功移除的元素的数量(不包括被忽略的元素)。
	 *********************************************************************************/
	public Long remove(K key, Object... values) {
		try {
			return ops.remove(key, values);
		} catch(Exception e) {
		}
		return -1L;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取指定集合的并集。
	 * 不存在的集合 key 被视为空集。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @return 并集成员的列表
	 *********************************************************************************/
	public Set<V> union(K key, Collection<K> otherKeys) {
		return ops.union(key, otherKeys);
	} 
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code union(K key, Collection<K> otherKeys)} 方法
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @return 并集成员的列表
	 *********************************************************************************/
	public Set<V> union(K key, K otherKey) {
		return ops.union(key, otherKey);
	} 
	
	/**********************************************************************************
	 * <PRE>
	 * 将制定集合的并集存储到目标集合中。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @param destinationKey
	 * @return 目标集合的元素数量
	 *********************************************************************************/
	public Long unionStore(K key, Collection<K> otherKeys, K destinationKey) {
		return ops.unionAndStore(key, otherKeys, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code unionStore(K key, Collection<K> otherKeys, K destinationKey)} 方法
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @param destinationKey
	 * @return 目标集合的元素数量
	 *********************************************************************************/
	public Long unionStore(K key, K otherKey, K destinationKey) {
		return ops.unionAndStore(key, otherKey, destinationKey);
	}
}
