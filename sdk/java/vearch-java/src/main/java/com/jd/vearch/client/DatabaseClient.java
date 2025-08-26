package com.jd.vearch.client;

import com.jd.vearch.model.Database;
import com.jd.vearch.model.Space;

import java.io.IOException;
import java.util.List;

/**
 * @Author liujishuai
 * @DATE 2025/08/01 8:00 PM
 */
public interface DatabaseClient {

    /**
     * 获取所有数据库
     * @return
     * @throws IOException
     */
    List<Database> getAllDatabases();

    /**
     * 创建数据库
     * @param databaseName
     * @throws IOException
     */
    Database createDatabase(String databaseName);
    /**
     * 查看数据库
     * @param databaseName
     * @return
     * @throws IOException
     */
    Database viewDatabase(String databaseName);

    /**
     * 删除数据库
     * @param databaseName
     * @throws IOException
     */
    boolean deleteDatabase(String databaseName);

    /**
     * 获取数据库下所有表空间
     * @param databaseName
     * @return
     * @throws IOException
     */
    List<Space> getAllSpaces(String databaseName);
}
