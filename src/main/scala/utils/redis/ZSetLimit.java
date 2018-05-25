package utils.redis;

import org.springframework.data.redis.connection.RedisZSetCommands;

/**
 * <P></P>
 * @author SEAISLAND
 */
public class ZSetLimit 
extends RedisZSetCommands.Limit
{
	public ZSetLimit() {
		super();
	}
}
