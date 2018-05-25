package utils.redis;

import org.springframework.data.redis.core.DefaultTypedTuple;

/**
 * <PRE>
 * 有序集合元组
 * </PRE>
 * @author SEAISLAND
 * @param <V>
 * @version 1.0.0
 * @since 1.0.0
 */
public class ZSetTuple<V> 
extends DefaultTypedTuple<V>
{
	public ZSetTuple(V value, Double score) {
		super(value, score);
	}
}
