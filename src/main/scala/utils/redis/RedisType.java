package utils.redis;

/**
 * <P>REDIS数据类型定义</P>
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public enum RedisType {
	
	/** 哈希 */
	HASH,
	
	/** 列表 */
	LIST,
	
	/** 集合 */
	SET,
	
	/** 排序集合 */
	SORTED_SET,
	
	/** 字符串 */
	STRING,
	
	/** 未知 */
	NONE
}
