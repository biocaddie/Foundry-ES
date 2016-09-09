package org.neuinfo.foundry.consumers.common;

/**
 * Created by bozyurt on 4/19/16.
 */
public interface ICacheManager<K,V> {
    public V get(K key);
    public void put(K key, V value);
    public void shutdown();
}
