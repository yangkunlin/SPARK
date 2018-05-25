package utils.redis;

import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.JedisPoolConfig;
import utils.IoUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <PRE>
 * 基于［Spring Data for Redis］和［Jedis］实现。
 * 需在当前应用的CLASSPATH根路径下放置 redis.properties 配置文件。
 * <p>
 * 文件格式如下:
 * spring.redis.cluster.nodes=10.174.92.11:6300,10.174.93.182:6300,10.45.146.63:6300
 * spring.redis.cluster.timeout=5
 * spring.redis.cluster.max-redirects=3
 * spring.redis.cluster.password=
 * </p>
 * jedis.pool.config.block_when_exhausted=true
 * jedis.pool.config.eviction_policy_classname=org.apache.commons.pool2.impl.DefaultEvictionPolicy
 * jedis.pool.config.fairness=false
 * jedis.pool.config.jmx_enabled=false
 * jedis.pool.config.jmx_name_base=ZONETRY
 * jedis.pool.config.jmx_name_prefix=redis-cluster-pool
 * jedis.pool.config.lifo=true
 * jedis.pool.config.max_wait_milliseconds=-1
 * jedis.pool.config.min_evictable_idletime_milliseconds=60000
 * jedis.pool.config.num_tests_per_eviction_run=-1
 * jedis.pool.config.soft_min_evictable_idletime_milliseconds=1800000
 * jedis.pool.config.test_on_borrow=false
 * jedis.pool.config.test_on_create=false
 * jedis.pool.config.test_on_return=false
 * jedis.pool.config.test_while_idle=true
 * jedis.pool.config.time_between_eviction_runs_milliseconds=30000
 * jedis.pool.config.min_idle=8
 * jedis.pool.config.max_idle=16
 * jedis.pool.config.max_total=1024
 * </PRE>
 *
 * @author SEAISLAND
 * @version 1.0.0
 * @since 1.0.0
 */
public final class RedisUtil {

    private final static String _cfgfile = "redis.properties";
    private static JedisConnectionFactory _factory = null;

    static {
        init();
    }

    /**********************************************************************************
     * <PRE>
     * 连接REDIS集群.
     * 返回REDIS集群客户端.
     * 当建立连接失败时, 返回NULL.
     * </PRE>
     * @param <K> KEY类型
     * @param <V> VALUE类型
     * @return {@link RedisClient}
     *********************************************************************************/
    @SuppressWarnings("unchecked")
    public static <K, V> RedisClient<K, V> open() {
        if (_factory == null) {
            init();
        }
        try {
            RedisTemplate<K, V> template = new RedisTemplate<>();
            template.setConnectionFactory(_factory);
            template.afterPropertiesSet();
            return new RedisClient(template);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**********************************************************************************
     * <P>释放REDIS集群连接资源</P>
     *********************************************************************************/
    public static void destroy() {
        if (_factory != null) {
            try {
                _factory.destroy();
            } catch (Exception e) {
                e.printStackTrace();
            }
            _factory = null;
        }
    }

    /**********************************************************************************
     * <P>初始化REDIS集群客户端环境</P>
     *********************************************************************************/
    private static void init() {
        //加载配置文件
        InputStream cfgis = Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(_cfgfile);
        if (cfgis == null) {
            System.err.println(String.format("Can not find %s", _cfgfile));
            return;
        }

        Properties cfgprops = new Properties();
        try {
            cfgprops.load(cfgis);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } finally {
            IoUtil.closeInputStream(cfgis);
        }

        // REDIS 集群配置
        RedisClusterConfiguration cfg = new RedisClusterConfiguration(
            new PropertiesPropertySource("Redis", cfgprops)
        );

        // REDIS 连接池配置
        JedisPoolConfig poolcfg = new JedisPoolConfig();

        if (cfgprops.containsKey("jedis.pool.config.block_when_exhausted")) {
            try {
                poolcfg.setBlockWhenExhausted(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.block_when_exhausted")
                ));
            } catch (Exception e) {
                poolcfg.setBlockWhenExhausted(true);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.eviction_policy_classname")) {
            poolcfg.setEvictionPolicyClassName(
                cfgprops.getProperty("jedis.pool.config.eviction_policy_classname")
            );
        }
        if (cfgprops.containsKey("jedis.pool.config.fairness")) {
            try {
                poolcfg.setFairness(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.fairness")
                ));
            } catch (Exception e) {
                poolcfg.setFairness(false);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.jmx_enabled")) {
            try {
                poolcfg.setJmxEnabled(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.jmx_enabled")
                ));
            } catch (Exception e) {
                poolcfg.setJmxEnabled(false);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.jmx_name_base")) {
            poolcfg.setJmxNameBase(cfgprops.getProperty("jedis.pool.config.jmx_name_base"));
        }
        if (cfgprops.containsKey("jedis.pool.config.jmx_name_prefix")) {
            poolcfg.setJmxNamePrefix(cfgprops.getProperty("jedis.pool.config.jmx_name_prefix"));
        }
        if (cfgprops.containsKey("jedis.pool.config.lifo")) {
            try {
                poolcfg.setLifo(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.lifo")
                ));
            } catch (Exception e) {
                poolcfg.setLifo(true);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.max_wait_milliseconds")) {
            try {
                poolcfg.setMaxWaitMillis(Long.parseLong(
                    cfgprops.getProperty("jedis.pool.config.max_wait_milliseconds")
                ));
            } catch (Exception e) {
                poolcfg.setMaxWaitMillis(-1L);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.min_evictable_idletime_milliseconds")) {
            try {
                poolcfg.setMinEvictableIdleTimeMillis(Long.parseLong(
                    cfgprops.getProperty("jedis.pool.config.min_evictable_idletime_milliseconds")
                ));
            } catch (Exception e) {
                poolcfg.setMaxWaitMillis(60000L);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.num_tests_per_eviction_run")) {
            try {
                poolcfg.setNumTestsPerEvictionRun(Integer.parseInt(
                    cfgprops.getProperty("jedis.pool.config.num_tests_per_eviction_run")
                ));
            } catch (Exception e) {
                poolcfg.setNumTestsPerEvictionRun(-1);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.soft_min_evictable_idletime_milliseconds")) {
            try {
                poolcfg.setSoftMinEvictableIdleTimeMillis(Long.parseLong(
                    cfgprops.getProperty("jedis.pool.config.soft_min_evictable_idletime_milliseconds")
                ));
            } catch (Exception e) {
                poolcfg.setSoftMinEvictableIdleTimeMillis(-1L);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.test_on_borrow")) {
            try {
                poolcfg.setTestOnBorrow(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.test_on_borrow")
                ));
            } catch (Exception e) {
                poolcfg.setTestOnBorrow(false);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.test_on_create")) {
            try {
                poolcfg.setTestOnCreate(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.test_on_create")
                ));
            } catch (Exception e) {
                poolcfg.setTestOnCreate(false);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.test_on_return")) {
            try {
                poolcfg.setTestOnReturn(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.test_on_return")
                ));
            } catch (Exception e) {
                poolcfg.setTestOnReturn(false);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.test_while_idle")) {
            try {
                poolcfg.setTestWhileIdle(Boolean.parseBoolean(
                    cfgprops.getProperty("jedis.pool.config.test_while_idle")
                ));
            } catch (Exception e) {
                poolcfg.setTestWhileIdle(true);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.time_between_eviction_runs_milliseconds")) {
            try {
                poolcfg.setTimeBetweenEvictionRunsMillis(Long.parseLong(
                    cfgprops.getProperty("jedis.pool.config.time_between_eviction_runs_milliseconds")
                ));
            } catch (Exception e) {
                poolcfg.setSoftMinEvictableIdleTimeMillis(30000L);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.max_idle")) {
            try {
                poolcfg.setMaxIdle(Integer.parseInt(
                    cfgprops.getProperty("jedis.pool.config.max_idle")
                ));
            } catch (Exception e) {
                poolcfg.setMaxIdle(16);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.min_idle")) {
            try {
                poolcfg.setMinIdle(Integer.parseInt(
                    cfgprops.getProperty("jedis.pool.config.min_idle")
                ));
            } catch (Exception e) {
                poolcfg.setMinIdle(8);
            }
        }
        if (cfgprops.containsKey("jedis.pool.config.max_total")) {
            try {
                poolcfg.setMaxTotal(Integer.parseInt(
                    cfgprops.getProperty("jedis.pool.config.max_total")
                ));
            } catch (Exception e) {
                poolcfg.setMaxTotal(1024);
            }
        }

        // 创建连接工厂
        _factory = new JedisConnectionFactory(cfg, poolcfg);
        _factory.afterPropertiesSet();
    }

}
