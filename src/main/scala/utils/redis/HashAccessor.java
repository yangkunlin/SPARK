package utils.redis;

import org.springframework.data.redis.core.HashOperations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <P>哈希表访问器</P>
 *
 * @param <K>  KEY类型
 * @param <HK> 字段KEY类型
 * @param <HV> 字段VALUE类型
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public final class HashAccessor<K, HK, HV> {

    private HashOperations<K, HK, HV> ops;

    HashAccessor(HashOperations<K, HK, HV> hops) {
        this.ops = hops;
    }

    /**********************************************************************************
     * <PRE>
     * 删除哈希表 key 中的一个或多个指定字段。
     * 不存在的字段将被忽略。
     * </PRE>
     * @param key
     * @param hkeys
     * @return 被成功删除字段的数量(不包括被忽略的字段)。
     *********************************************************************************/
    @SuppressWarnings("unchecked")
    public Long delete(K key, HK... hkeys) {
        return ops.delete(key, new Object[]{hkeys});
    }

    /**********************************************************************************
     * <PRE>
     * 获取在哈希表中指定 key 的所有字段和值。
     * 若 key 不存在，返回空列表。
     * </PRE>
     * @param key
     * @return
     *********************************************************************************/
    public Map<HK, HV> getall(K key) {
        return ops.entries(key);
    }

    /**********************************************************************************
     * <PRE>
     * 获取哈希表中指定字段的值。
     * 若 hkey 或 key 不存在时，返回 NULL 。
     * </PRE>
     * @param key
     * @param hkey
     * @return
     *********************************************************************************/
    public HV get(K key, HK hkey) {
        return ops.get(key, hkey);
    }

    /**********************************************************************************
     * <PRE>
     * 查看哈希表的指定的字段是否存在。
     * 若哈希表含有指定字段，返回 TRUE 。
     * 若哈希表不含有指定字段，或 key 不存在时，返回 FALSE 。
     * </PRE>
     * @param key
     * @param hkey
     * @return
     *********************************************************************************/
    public Boolean exists(K key, HK hkey) {
        return ops.hasKey(key, hkey);
    }

    /**********************************************************************************
     * <PRE>
     * 为哈希表中的指定字段值加上浮点数增量值。
     * 若指定的字段不存在，在执行命令前，字段的值被初始化为 0 。
     * </PRE>
     * @param key
     * @param hkey
     * @param delta
     * @return 执行增量之后哈希表中字段的值
     *********************************************************************************/
    public Double increment(K key, HK hkey, double delta) {
        return ops.increment(key, hkey, delta);
    }

    /**********************************************************************************
     * <PRE>
     * 为哈希表中的指定字段值加上增量值。
     * 增量也可以为负数，相当于对指定字段进行减法操作。
     * 若哈希表的 key 不存在，新的哈希表被创建，并执行增量操作。
     * 若指定的字段不存在，在执行命令前，字段的值被初始化为 0 。
     * 对一个储存字符串值的字段执行增量操作，将造成一个错误。
     * </PRE>
     * @param key
     * @param hkey
     * @param delta
     * @return 执行增量之后哈希表中字段的值
     *********************************************************************************/
    public Long increment(K key, HK hkey, long delta) {
        return ops.increment(key, hkey, delta);
    }

    /**********************************************************************************
     * <PRE>
     * 获取哈希表中的所有字段名。
     * 当 key 不存在时，返回一个空列表。
     * </PRE>
     * @param key
     * @return
     *********************************************************************************/
    public Set<HK> keys(K key) {
        return ops.keys(key);
    }

    /**********************************************************************************
     * <PRE>
     * 获取哈希表中多个给定字段的值。
     * 若指定的某字段不存在，对应该字段返回一个 NULL 值。
     * 字段值列表的排列顺序和指定字段的请求顺序一样。
     * </PRE>
     * @param key
     * @param hkeys
     * @return
     *********************************************************************************/
    public List<HV> getm(K key, Collection<HK> hkeys) {
        return ops.multiGet(key, hkeys);
    }

    /**********************************************************************************
     * <PRE>
     * 为哈希表中的指定字段赋值。
     * 若哈希表不存在，新的哈希表被创建，并进行赋值操作。
     * 若指定的字段已存在，旧值将被覆盖。
     * </PRE>
     * @param key
     * @param hkey
     * @param hvalue
     *********************************************************************************/
    public void set(K key, HK hkey, HV hvalue) {
        ops.put(key, hkey, hvalue);
    }

    /**********************************************************************************
     * <PRE>
     * 同时将多个 field-value (字段-值)设置到哈希表中。
     * 已存在的字段将被覆盖。
     * 若哈希表不存在，创建一个空哈希表，并执行赋值操作。
     * </PRE>
     * @param key
     * @param hmap
     *********************************************************************************/
    public void setm(K key, Map<? extends HK, ? extends HV> hmap) {
        ops.putAll(key, hmap);
    }

    /**********************************************************************************
     * <PRE>
     * 为哈希表中不存在的的字段赋值。
     * 若哈希表不存在，新的哈希表被创建，并进行赋值操作。
     * 若指定的字段已经存在，操作无效，返回 FALSE 。
     * 设置成功, 返回 TRUE 。
     * </PRE>
     * @param key
     * @param hkey
     * @param hvalue
     * @return
     *********************************************************************************/
    public Boolean setnx(K key, HK hkey, HV hvalue) {
        return ops.putIfAbsent(key, hkey, hvalue);
    }

    /**********************************************************************************
     * <PRE>
     * 获取哈希表中字段的数量。
     * 若哈希表不存在, 返回 0 。
     * </PRE>
     * @param key
     * @return
     *********************************************************************************/
    public Long size(K key) {
        return ops.size(key);
    }

    /**********************************************************************************
     * <PRE>
     * 获取哈希表中所有字段的值。
     * 当 key 不存在时，返回一个空表。
     * </PRE>
     * @param key
     * @return
     *********************************************************************************/
    public List<HV> values(K key) {
        return ops.values(key);
    }
}
