package utils.redis;

import org.springframework.data.redis.connection.RedisClusterNode;

/**
 * <P>集群节点</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public class ClusterNode 
extends RedisClusterNode
{
	public ClusterNode() { 
		super(); 
	}
	public ClusterNode(ClusterSlotRange range) { 
		super(range); 
	}
	public ClusterNode(String id) {
		super(id); 
	}
	public ClusterNode(String host, int port) {
		super(host, port); 
	}
	public ClusterNode(String host, int port, ClusterSlotRange range) {
		super(host, port, range); 
	}
}
