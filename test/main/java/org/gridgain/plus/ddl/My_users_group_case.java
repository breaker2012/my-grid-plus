package org.gridgain.plus.ddl;

import cn.plus.model.MyGroupSqlType;
import cn.plus.model.MyUsersGroup;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class My_users_group_case {

    @Test
    public void test_init_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<?, ?> template_cfg = new CacheConfiguration<>("MyMeta_template*").setSqlSchema("MY_META");
        template_cfg.setCacheMode(CacheMode.REPLICATED);
        template_cfg.setReadFromBackup(true);
        ignite.addCacheConfiguration(template_cfg);

        //CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        String sql = "DROP TABLE IF EXISTS table_index_item";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS table_index_item (" +
                "                id BIGINT," +
                "                index_item VARCHAR(50)," +
                "                sort_order VARCHAR(4)," +
                "                index_no BIGINT," +
                "                PRIMARY KEY (id, index_no)" +
                ") WITH \"template=MyMeta_template,cache_name=table_index_item,affinityKey=index_no,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndexItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\"";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    @Test
    public void test_init() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<?, ?> template_cfg = new CacheConfiguration<>("MyMeta_template*").setSqlSchema("MY_META");
        template_cfg.setCacheMode(CacheMode.REPLICATED);
        template_cfg.setReadFromBackup(true);
        ignite.addCacheConfiguration(template_cfg);

        //CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        String sql = "DROP TABLE IF EXISTS table_index";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS table_index (" +
                "                id BIGINT," +
                "                index_name VARCHAR(50)," +
                "                spatial BOOLEAN DEFAULT false," +
                "                table_id BIGINT," +
                "                PRIMARY KEY (id, table_id)" +
                ") WITH \"template=MyMeta_template,cache_name=table_index,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndex,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\"";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    @Test
    public void test_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<?, ?> template_cfg = new CacheConfiguration<>("MyMeta_template*").setSqlSchema("MY_META");
        template_cfg.setCacheMode(CacheMode.REPLICATED);
        template_cfg.setReadFromBackup(true);
        ignite.addCacheConfiguration(template_cfg);

        //CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        String sql = "DROP TABLE IF EXISTS my_users_group";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_users_group (" +
                "                  id BIGINT," +
                "                  group_name VARCHAR(40)," +
                "                  data_set_id BIGINT DEFAULT 0," +
                "                  group_type VARCHAR(8)," +
                "                  PRIMARY KEY (id)" +
                "                ) WITH \"template=MyMeta_template,cache_name=my_users_group,VALUE_TYPE=cn.plus.model.MyUsersGroup,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\"";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    @Test
    public void case_2() throws Exception {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        ignite.cache("my_users_group").remove(1L);
        ignite.cache("my_users_group").remove(2L);
        MyUsersGroup myUsersGroup = new MyUsersGroup();
        myUsersGroup.setId(1L);
        myUsersGroup.setData_set_id(1L);
        myUsersGroup.setGroup_name("测试一");
        myUsersGroup.setMyGroupSqlType(MyGroupSqlType.ALL);
        ignite.cache("my_users_group").put(1L, myUsersGroup);

        String sql = "insert into my_users_group (id, group_name, data_set_id, group_type) values (2, '测试2', 2, 'ALL')";
        ignite.cache("my_users_group").query(new SqlFieldsQuery(sql)).getAll();
    }

    @Test
    public void case_3() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        MyUsersGroup myUsersGroup = (MyUsersGroup) ignite.cache("my_users_group").get(1L);
        System.out.println(myUsersGroup.getGroup_type());
    }
}
