package com.jd.vearch.client;

import com.jd.vearch.model.Cache;
import com.jd.vearch.model.Space;

import java.io.IOException;
import java.util.List;

/**
 * @Author liujishuai
 * @DATE 2025/8/1 8:01 PM
 */
public interface SpaceClient {

    /**
     * 创建表空间
     * @param space
     * @throws IOException
     */
    String createSpace(Space space) ;

    /**
     * 查看表空间
     * @param spaceName
     * @return
     * @throws IOException
     */
    String viewSpace(String spaceName) ;
    /**
     * 删除表空间
     * @param spaceName
     * @throws IOException
     */
    String deleteSpace(String spaceName);

    /**
     * 修改表空间缓存大小
     * @param spaceName
     * @param caches
     * @throws IOException
     */
    String modifyCacheSize(String spaceName, List<Cache> caches);
    /**
     * 查看表空间缓存大小
     * @param spaceName
     * @return
     * @throws IOException
     */
    String viewCacheSize(String spaceName) ;
}
