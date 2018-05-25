package utils.redis;

import org.springframework.data.redis.core.ZSetOperations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * <P>排序Set数据类型访问器</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @since 1.0.0
 */
public class ZSetAccessor<K,V> {
	
	private ZSetOperations<K,V> ops = null;
	
	ZSetAccessor(ZSetOperations<K,V> zsetops) {
		this.ops = zsetops;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 将一个成员元素及其分数值加入到有序集当中。
	 * 成功返回 TRUE ，失败返回 FALSE 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @param score
	 * @return 
	 *********************************************************************************/
	public Boolean add(K key, V value, double score) {
		return ops.add(key, value, score);
	}

	/**********************************************************************************
	 * <PRE>
	 * 将一个或多个成员元素及其分数值加入到有序集当中。
	 * 若某成员已存在于有序集合中，则更新该成员的分数值，并重新插入该成员，以保证该成员在正确位置上。
	 * 分数值可以是整数或双精度浮点数。
	 * 若有序集合 key 不存在，则创建一个空的有序集合并执行插入操作。
	 * 当 key 存在但不是有序集合类型时，发生错误，并返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param tuples
	 * @return 被成功添加的新成员的数量（不包括那些被更新的已经存在的成员）
	 *********************************************************************************/
	public Long add(K key, Set<ZSetTuple<V>> tuples) {
		Set<ZSetOperations.TypedTuple<V>> __tuples = new HashSet<>();
		Iterator<ZSetTuple<V>> _tuples = tuples.iterator();
		while(_tuples.hasNext()) {
			__tuples.add(_tuples.next());
		}
		try {
			return ops.add(key, __tuples);
		} catch (Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 计算有序集合中指定分数区间的成员数量。
	 * 失败时返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @return 分数值在 min 和 max 之间的成员的数量
	 *********************************************************************************/
	public Long count(K key, double min, double max) {
		try {
			return ops.count(key, min, max);
		} catch (Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 对有序集合中指定成员的分数加上增量值。
	 * 可以通过传递一个负数值，让分数减去相应的值。
	 * 当 key 不存在时，执行 ADD 操作。
	 * 当 key 不是有序集合类型时，发生错误，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param value
	 * @param delta
	 * @return 成员的新分数值
	 *********************************************************************************/
	public Double increment(K key, V value, double delta) {
		try {
			return ops.incrementScore(key, value, delta);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 计算指定的一个或多个有序集合的交集，并将该交集(结果集)储存到目标集合中。
	 * 默认情况下，结果集中某个成员的分数值是所有指定集合下该成员分数值之和。
	 * 执行失败时，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @param destinationKey
	 * @return 保存到目标结果集的的成员数量
	 *********************************************************************************/
	public Long intersectStore(K key, Collection<K> otherKeys, K destinationKey) {
		try {
			return ops.intersectAndStore(key, otherKeys, destinationKey);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code intersectStore(K, Collection<K>, K) } 方法。
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @param destinationKey
	 * @return 
	 *********************************************************************************/
	public Long intersectStore(K key, K otherKey, K destinationKey) {
		try {
			return ops.intersectAndStore(key, otherKey, destinationKey);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定区间内的成员。
	 * 结果集合成员的位置按分数值递增(从小到大)来排序。
	 * 具有相同分数值的成员按字典顺序来排列。
	 * 下标参数 start 和 end ，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
	 * 也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end
	 * @return 指定区间内带有分数值(可选)的有序集合成员的列表
	 *********************************************************************************/
	public Set<V> range(K key, long start, long end) {
		return ops.range(key, start, end);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定区间内的成员。
	 * 结果集合成员的位置按分数值递减(从大到小)来排序。
	 * 参照 {@code range(K key, long start, long end) } 方法。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end
	 * @return 
	 *********************************************************************************/
	public Set<V> xrange(K key, long start, long end) {
		return ops.reverseRange(key, start, end);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 通过字典区间返回有序集合的成员。
	 * </PRE>
	 * @param key
	 * @param range
	 * @return 指定区间内的元素列表
	 *********************************************************************************/
	public Set<V> rangeLexico(K key, ZSetRange range) {
		return ops.rangeByLex(key, range);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * </PRE>
	 * @param key
	 * @param range
	 * @param limit
	 * @return 
	 *********************************************************************************/
	public Set<V> rangeLexico(K key, ZSetRange range, ZSetLimit limit) {
		return ops.rangeByLex(key, range, limit);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定分数区间的成员列表。
	 * 有序集成员按分数值递增(从小到大)顺序排列。
	 * 具有相同分数值的成员按字典序来排列(该属性是有序集合提供的，不需要额外的计算)。
	 * 默认情况下，区间的取值使用闭区间 (≤ 或 ≥); 也可以通过给参数前增加“(”符号来表示开区间 (小于或大于)。
	 * 失败时, 返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @return 
	 *********************************************************************************/
	public Set<V> rangeScore(K key, double min, double max) {
		try {
			return ops.rangeByScore(key, min, max);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定分数区间的成员列表。
	 * 结果集合成员按分数值递减(从大到小)顺序排列。
	 * 参照 {@code rangeScore(K key, double min, double max) } 方法。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @return 
	 *********************************************************************************/
	public Set<V> xrangeScore(K key, double min, double max) {
		try {
			return ops.reverseRangeByScore(key, min, max);
		} catch(Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中, 从指定偏移量(OFFSET)开始, COUNT数量的, 特定分数区间的成员列表。
	 * 参照 {@code rangeScore(K key, double min, double max) } 方法。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return 
	 *********************************************************************************/
	public Set<V> rangeScore(K key, double min, double max, long offset, long count) {
		try {
			return ops.rangeByScore(key, min, max, offset, count);
		} catch (Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中, 从指定偏移量(OFFSET)开始, COUNT数量的, 特定分数区间的成员列表。
	 * 结果集合成员按分数值递减(从大到小)顺序排列。
	 * 参照 {@code xrangeScore(K key, double min, double max) } 方法。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return 
	 *********************************************************************************/
	public Set<V> xrangeScore(K key, double min, double max, long offset, long count) {
		try {
			return ops.reverseRangeByScore(key, min, max, offset, count);
		} catch (Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定成员的排名。
	 * 其中有序集成员按分数值递增(从小到大)顺序排列。
	 * 如果成员是有序集合 key 的成员，返回该成员的排名。
	 * 如果成员不是有序集合 key 的成员，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param o
	 * @return 
	 *********************************************************************************/
	public Long rank(K key, Object o) {
		return ops.rank(key, o);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中指定成员的排名。
	 * 其中有序集成员按分数值递减(从大到小)顺序排列。
	 * 参照 {@code rank(K key, Object o) } 方法。
	 * </PRE>
	 * @param key
	 * @param o
	 * @return 
	 *********************************************************************************/
	public Long xrank(K key, Object o) {
		return ops.rank(key, o);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除有序集合中的一个或多个成员，不存在的成员将被忽略。
	 * 当 key 存在但不是有序集类型时，发生错误，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param values
	 * @return 被成功移除的成员的数量（不包括被忽略的成员）
	 *********************************************************************************/
	public Long remove(K key, Object... values) {
		try {
			return ops.remove(key, values);
		} catch (Exception e) {
		}
		return null;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除有序集合中指定区间的所有成员。
	 * </PRE>
	 * @param key
	 * @param start
	 * @param end
	 * @return 被成功移除的成员的数量
	 *********************************************************************************/
	public Long removeRange(K key, long start, long end) {
		return ops.removeRange(key, start, end);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 移除有序集合中，指定分数（SCORE）区间内的所有成员。
	 * </PRE>
	 * @param key
	 * @param min
	 * @param max
	 * @return 被移除成员的数量
	 *********************************************************************************/
	public Long removeScore(K key, double min, double max) {
		return ops.removeRangeByScore(key, min, max);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中某成员的分数值。
	 * 如果成员元素不是有序集 key 的成员，或 key 不存在，返回 NULL 。
	 * </PRE>
	 * @param key
	 * @param o
	 * @return 
	 *********************************************************************************/
	public Double score(K key, Object o) {
		return ops.score(key, o);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 获取有序集合中元素的数量。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public Long size(K key) {
		return ops.size(key);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 计算一个或多个有序集合的并集，并将该并集(结果集)储存到目标集合中。
	 * 默认情况下，目标集合中某成员的分数值是所有指定集合下该成员的分数值之和。
	 * </PRE>
	 * @param key
	 * @param otherKeys
	 * @param destinationKey
	 * @return 保存到结果集的成员数量
	 *********************************************************************************/
	public Long unionstore(K key, Collection<K> otherKeys, K destinationKey) {
		return ops.unionAndStore(key, otherKeys, destinationKey);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 参照 {@code unionstore(K, Collection<K>, K)} 方法。
	 * </PRE>
	 * @param key
	 * @param otherKey
	 * @param destinationKey
	 * @return 保存到结果集的成员数量
	 *********************************************************************************/
	public Long unionstore(K key, K otherKey, K destinationKey) {
		return ops.unionAndStore(key, otherKey, destinationKey);
	} 
	
	/**********************************************************************************
	 * <PRE>
	 * 计算有序集合中元素的数量。
	 * 当 key 存在且是有序集类型时，返回有序集合的基数。
	 * 当 key 不存在时，返回 0 。
	 * </PRE>
	 * @param key
	 * @return 
	 *********************************************************************************/
	public Long zcard(K key) {
		return ops.zCard(key);
	}
}
