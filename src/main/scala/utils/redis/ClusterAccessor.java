package utils.redis;

import org.springframework.data.redis.core.ClusterOperations;

/**
 * <P>集群属性访问器</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @since 1.0.0
 */
public final class ClusterAccessor<K,V> 
{
	private ClusterOperations<K,V> ops = null;
	
	ClusterAccessor(ClusterOperations<K,V> clusterops) {
		this.ops = clusterops;
	}
	
	/**********************************************************************************
	 * <PRE>
	 * Add slots to given node.
	 * </PRE>
	 * @param node
	 * @param slots 
	 *********************************************************************************/
	public void	addSlots(ClusterNode node, int... slots) {
		ops.addSlots(node, slots);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * Add slots in {@link ClusterSlotRange} to given node.
	 * </PRE>
	 * @param node
	 * @param range 
	 *********************************************************************************/
	public void	addSlots(ClusterNode node, ClusterSlotRange range) {
		ops.addSlots(node, range);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * Start an Append Only File rewrite process on given node.
	 * 用于异步执行一个 AOF（AppendOnly File）文件重写操作。
	 * 重写会创建一个当前 AOF 文件的体积优化版本。
	 * 即使执行失败，也不会有任何数据丢失，因为旧的 AOF 文件在此命令成功之前不会被修改。
	 * </PRE>
	 * @param node 
	 *********************************************************************************/
	public void backgroundRewriteAOF(ClusterNode node) {
		ops.bgReWriteAof(node);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * Start background saving of db on given node.
	 * 在后台异步保存当前数据库的数据到磁盘。
	 * 命令执行之后立即返回，然后 Redis fork 出一个新子进程，原来的 Redis 进程(父进程)
	 * 继续处理客户端请求，而子进程则负责将数据保存到磁盘，然后退出。
	 * </PRE>
	 * @param node 
	 *********************************************************************************/
	public void	backgroundSave(ClusterNode node) {
		ops.bgSave(node);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * 清空当前数据库中的所有 key 。
	 * </PRE>
	 * @param node 
	 *********************************************************************************/
	public void flushdb(ClusterNode node) {
		ops.flushDb(node);
	}
	
	/**********************************************************************************
	 * <PRE>
	 * Remove the node from the cluster.
	 * </PRE>
	 * @param node 
	 *********************************************************************************/
	public void forget(ClusterNode node) {
		ops.forget(node);
	}
	
}
