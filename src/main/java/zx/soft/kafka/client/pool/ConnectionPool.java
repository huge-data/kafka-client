package zx.soft.kafka.client.pool;

import java.io.Serializable;

/**
 * 连接池接口
 *
 * @author wanggang
 *
 * @param <T>
 */
public interface ConnectionPool<T> extends Serializable {

	/**
	 * 获取连接
	 *
	 * @return 连接
	 */
	public abstract T getConnection();

	/**
	 * 返回连接
	 *
	 * @param conn 连接
	 */
	public void returnConnection(T conn);

	/**
	 * 废弃连接
	 *
	 * @param conn 连接
	 */
	public void invalidateConnection(T conn);

}
