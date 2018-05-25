package utils.redis;

import org.springframework.data.redis.connection.RedisClusterNode;

import java.util.Collection;

/**
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public class ClusterSlotRange 
extends RedisClusterNode.SlotRange
{
	
	public ClusterSlotRange(Integer lowerBound, Integer upperBound) {
		super(lowerBound, upperBound);
	}
	
	public ClusterSlotRange(Collection<Integer> range) {
		super(range);
	}
}
