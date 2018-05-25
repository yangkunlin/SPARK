package utils.redis;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <PRE>
 * REDIS集群客户端.
 * (对Spring的{@link RedisTemplate}进行了基本封装)
 * </PRE>
 *
 * @param <K> KEY类型
 * @param <V> VALUE类型
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public class RedisClient<K, V> {

    private RedisTemplate<K, V> redis;

    RedisClient(RedisTemplate<K, V> template) {
        redis = template;
    }

    /**********************************************************************************
     * <P>获得 {@link RedisTemplate} 实例</P>
     * @return {@link RedisTemplate}
     *********************************************************************************/
    public final RedisTemplate<K, V> getRedisTemplate() {
        return redis;
    }

    /**********************************************************************************
     * <P>获得List数据类型访问器</P>
     * @return {@link ListAccessor}
     *********************************************************************************/
    public final ListAccessor<K, V> getListAccessor() {
        return new ListAccessor<>(redis.opsForList());
    }

    /**********************************************************************************
     * <P>获得Value数据类型访问器</P>
     * @return {@link ValueAccessor}
     *********************************************************************************/
    public final ValueAccessor<K, V> getValueAccessor() {
        return new ValueAccessor<>(redis.opsForValue());
    }

    /**********************************************************************************
     * <P>获得SortedSet数据类型访问器</P>
     * @return {@link ZSetAccessor}
     *********************************************************************************/
    public final ZSetAccessor<K, V> getSortedSetAccessor() {
        return new ZSetAccessor<>(redis.opsForZSet());
    }

    /**********************************************************************************
     * <P>获得Set数据类型访问器</P>
     * @return {@link SetAccessor}
     *********************************************************************************/
    public final SetAccessor<K, V> getSetAccessor() {
        return new SetAccessor<>(redis.opsForSet());
    }

    /**********************************************************************************
     * <P>获得Hash类型访问器</P>
     * @param <HK>
     * @param <HV>
     * @return {@link HashAccessor}
     *********************************************************************************/
    public final <HK, HV> HashAccessor<K, HK, HV> getHashAccessor() {
        return new HashAccessor<>(redis.opsForHash());
    }

    /**********************************************************************************
     * <P>获得集群访问器</P>
     * @return {@link ClusterAccessor}
     *********************************************************************************/
    public final ClusterAccessor<K, V> getClusterAccessor() {
        return new ClusterAccessor<>(redis.opsForCluster());
    }

    /**********************************************************************************
     * <P>删除指定的KEY</P>
     * @param key
     *********************************************************************************/
    public final void delete(K key) {
        redis.delete(key);
    }

    /**********************************************************************************
     * <P>删除指定的多个KEY</P>
     * @param keys
     *********************************************************************************/
    public final void delete(Collection<K> keys) {
        redis.delete(keys);
    }

    /**********************************************************************************
     * <PRE>
     * 执行Redis的dump命令.
     * 丢弃指定的KEY.
     * </PRE>
     * @param key
     * @return
     *********************************************************************************/
    public final byte[] dump(K key) {
        return redis.dump(key);
    }

    /**********************************************************************************
     * <P>设置指定KEY的过期时间</P>
     * @param key
     * @param timeout
     * @param unit
     * @return
     *********************************************************************************/
    public final Boolean expire(K key, long timeout, TimeUnit unit) {
        return redis.expire(key, timeout, unit);
    }

    /**********************************************************************************
     * <P>设置指定的KEY在特定的时间点过期</P>
     * @param key
     * @param date
     * @return
     *********************************************************************************/
    public final Boolean expireAt(K key, Date date) {
        return redis.expireAt(key, date);
    }

    /**********************************************************************************
     * <P>获取指定KEY的过期周期</P>
     * @param key
     * @return
     *********************************************************************************/
    public final Long getExpire(K key) {
        return redis.getExpire(key);
    }

    /**********************************************************************************
     * <P>以特定的时间单元获取指定KEY的过期周期</P>
     * @param key
     * @param unit
     * @return
     *********************************************************************************/
    public final Long getExpire(K key, TimeUnit unit) {
        return redis.getExpire(key, unit);
    }

    /**********************************************************************************
     * <P>判断指定的KEY是否存在</P>
     * @param key
     * @return
     *********************************************************************************/
    public final Boolean exists(K key) {
        return redis.hasKey(key);
    }

    /**********************************************************************************
     * <P>以特定的表达式模式, 查询出存在的KEY集合</P>
     * @param pattern
     * @return
     *********************************************************************************/
    public final Set<K> keys(K pattern) {
        return redis.keys(pattern);
    }

    /**********************************************************************************
     * <P>将指定的KEY移动到指定编号的库中</P>
     * @param key
     * @param dbindex
     * @return
     *********************************************************************************/
    public final Boolean move(K key, int dbindex) {
        return redis.move(key, dbindex);
    }

    /**********************************************************************************
     * <PRE>
     * 标记一个事务块的开始。
     * 事务块内的多条命令按照先后顺序被放进一个队列当中，最后由 EXEC 命令原子性地执行。
     * </PRE>
     *********************************************************************************/
    public final void multi() {
        redis.multi();
    }

    /**********************************************************************************
     * <PRE>
     * 执行所有事务块内的所有命令, 并恢复Redis连接至正常状态。
     * 当使用了WATCH命令时, exec()方法仅提交事务中受监视的KEY未被修改的那些命令.
     * </PRE>
     * @return
     *********************************************************************************/
    public final List<Object> exec() {
        return redis.exec();
    }

    /**********************************************************************************
     * <PRE>
     * 执行Redis的DISCARD命令, 用于取消事务，放弃执行事务块内的所有命令。
     * 如果执行了WATCH命令, discard()方法将用于解除所有受监视的KEY.
     * </PRE>
     *********************************************************************************/
    public final void discard() {
        redis.discard();
    }

    /**********************************************************************************
     * <P>持久化指定的KEY</P>
     * @param key
     * @return
     *********************************************************************************/
    public final Boolean persist(K key) {
        return redis.persist(key);
    }

    /**********************************************************************************
     * <P>获取一个随机KEY</P>
     * @return
     *********************************************************************************/
    public final K randomKey() {
        return redis.randomKey();
    }

    /**********************************************************************************
     * <P>重命名一个KEY</P>
     * @param oldKey
     * @param newKey
     *********************************************************************************/
    public final void rename(K oldKey, K newKey) {
        redis.rename(oldKey, newKey);
    }

    /**********************************************************************************
     * <P>重命名一个KEY(当新KEY名不存在时)</P>
     * @param oldKey
     * @param newKey
     * @return
     *********************************************************************************/
    public final Boolean renameOnAbsent(K oldKey, K newKey) {
        return redis.renameIfAbsent(oldKey, newKey);
    }

    /**********************************************************************************
     * <P>执行Redis的restore命令</P>
     * @param key
     * @param value
     * @param timeToLive
     * @param unit
     *********************************************************************************/
    public final void restore(K key, byte[] value, long timeToLive, TimeUnit unit) {
        redis.restore(key, value, timeToLive, unit);
    }

    /**********************************************************************************
     * <P>获取指定KEY的数据类型</P>
     * @param key
     * @return
     *********************************************************************************/
    public final RedisType type(K key) {
        DataType type = redis.type(key);
        switch (type) {
            case HASH:
                return RedisType.HASH;
            case LIST:
                return RedisType.LIST;
            case SET:
                return RedisType.SET;
            case ZSET:
                return RedisType.SORTED_SET;
            case STRING:
                return RedisType.STRING;
            default:
                return RedisType.NONE;
        }
    }

    /**********************************************************************************
     * <PRE>
     * 执行Redis的UNWATCH命令, 取消 WATCH 命令对所有 key 的监视。
     * 如果调用了exec()或discard()方法, 无需手动调用unwatch()方法。
     * </PRE>
     *********************************************************************************/
    public final void unwatch() {
        redis.unwatch();
    }

    /**********************************************************************************
     * <P>参照 {@code watch(Collection<K> keys)} 方法</P>
     * @param key
     *********************************************************************************/
    public final void watch(K key) {
        redis.watch(key);
    }

    /**********************************************************************************
     * <PRE>
     * 监视事务块中一个或多个key。
     * 如果在事务执行之前这个(或这些) key 被其他命令所改动，那么事务将被打断。
     * </PRE>
     * @param keys
     *********************************************************************************/
    public final void watch(Collection<K> keys) {
        redis.watch(keys);
    }
}
