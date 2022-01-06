(ns org.gridgain.plus.init.plus-init-sql)

; 1、超级用户名、密码直接写死 xml 文件中
; 2、初始化元表
(def my-grid-tables "
    /**
    1、用户组中添加 数据集 id
    group_type: DDL, DML, DDL和DML
    DROP TABLE IF EXISTS my_users_group;
    */
    CREATE TABLE IF NOT EXISTS my_users_group (
                    id BIGINT,
                    -- 用户组名称
                    group_name VARCHAR(40),
                    -- 数据集
                    data_set_id BIGINT DEFAULT 0,
                    -- 用户组类型
                    group_type VARCHAR(8),
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,cache_name=my_users_group,VALUE_TYPE=cn.plus.model.MyUsersGroup,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";
    
    /**
    2、数据集：my_dataset
    DROP TABLE IF EXISTS my_dataset;
    */
    CREATE TABLE IF NOT EXISTS my_dataset (
                    id BIGINT,
                    is_real BOOLEAN DEFAULT true,
                    dataset_name VARCHAR,
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,cache_name=my_dataset,VALUE_TYPE=cn.plus.model.ddl.MyDataSet,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    3、批处理数据集中的表：my_dataset_table
    DROP TABLE IF EXISTS my_dataset_table;
    */
    CREATE TABLE IF NOT EXISTS my_dataset_table (
                    id BIGINT,
                    table_name VARCHAR,
                    to_real BOOLEAN DEFAULT true,
                    dataset_id BIGINT,
                    PRIMARY KEY (id, dataset_id)
                    ) WITH \"template=MyMeta_template,cache_name=my_dataset_table,KEY_TYPE=cn.plus.model.ddl.MyDatasetTablePK,VALUE_TYPE=cn.plus.model.ddl.MyDatasetTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    4、批处理数据集中的表：my_dataset_table
    DROP TABLE IF EXISTS my_dataset_real_table;
    */
    CREATE TABLE IF NOT EXISTS my_dataset_real_table (
                    id BIGINT,
                    table_name VARCHAR,
                    dataset_id BIGINT,
                    PRIMARY KEY (id, dataset_id)
                    ) WITH \"template=MyMeta_template,cache_name=my_dataset_real_table,KEY_TYPE=cn.plus.model.ddl.MyDatasetTablePK,VALUE_TYPE=cn.plus.model.ddl.MyDatasetRealTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    5、存储元表： my_meta_tables
    DROP TABLE IF EXISTS my_meta_tables;
    */
    CREATE TABLE IF NOT EXISTS my_meta_tables (
                    id BIGINT,
                    table_name VARCHAR(50),
                    descrip VARCHAR,
                    --让创建 table 的语句不可见
                    --code VARCHAR,
                    data_set_id BIGINT,
                    PRIMARY KEY (id)
    ) WITH \"template=MyMeta_template,cache_name=my_meta_tables,VALUE_TYPE=cn.plus.model.ddl.MyTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    6、元表中记录表字段的表：table_item
    DROP TABLE IF EXISTS table_item;
    */
    CREATE TABLE IF NOT EXISTS table_item (
                    id BIGINT,
                    column_name VARCHAR(50),
                    column_len INT,
                    scale INT,
                    column_type VARCHAR(50),
                    not_null BOOLEAN DEFAULT true,
                    pkid BOOLEAN DEFAULT false,
                    comment VARCHAR(50),
                    auto_increment BOOLEAN DEFAULT false,
                    table_id BIGINT,
                    PRIMARY KEY (id, table_id)
    ) WITH \"template=MyMeta_template,cache_name=table_item,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    7、元表中记录表索引的表：table_index
    DROP TABLE IF EXISTS table_index;
    */
    CREATE TABLE IF NOT EXISTS table_index (
                    id BIGINT,
                    index_name VARCHAR(50),
                    spatial BOOLEAN DEFAULT false,
                    table_id BIGINT,
                    --ex_table_id_ BIGINT,
                    PRIMARY KEY (id, table_id)
    ) WITH \"template=MyMeta_template,cache_name=table_index,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndex,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    8、元表中记录表索引字段的表：table_index_item
    DROP TABLE IF EXISTS table_index_item;
    */
    CREATE TABLE IF NOT EXISTS table_index_item (
                    id BIGINT,
                    index_item VARCHAR(50),
                    sort_order VARCHAR(4),
                    index_no BIGINT,
                    -- table_id BIGINT,
                    PRIMARY KEY (id, index_no)
    ) WITH \"template=MyMeta_template,cache_name=table_index_item,affinityKey=index_no,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndexItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    9、元表中预先要建的索引
    DROP INDEX IF EXISTS my_meta_tables_idx;
    DROP INDEX IF EXISTS table_item_idx;
    DROP INDEX IF EXISTS table_index_idx;
    DROP INDEX IF EXISTS ot_ds_tname_idx;
    */
    CREATE INDEX IF NOT EXISTS my_meta_tables_idx ON my_meta_tables (table_name);
    CREATE INDEX IF NOT EXISTS table_item_idx ON table_item (table_id, column_name);
    CREATE INDEX IF NOT EXISTS table_index_idx ON table_index (table_id, index_name);

    /*
    data_set_id 和 table_name 唯一确定一个值
    添加 data_set_id 的作用是在该数据集的用户组可以查看该数据组的表集合
    DROP INDEX IF EXISTS ot_ds_tname_idx;
    */
    CREATE INDEX IF NOT EXISTS ot_ds_tname_idx ON my_meta_tables (table_name, data_set_id);

    /**
    10、场景表：my_scenes
    剔除原版本，状态，是否激活
    让 scenes_name 成为 key 值，同时新建一个 my_scenes_log 来记录整个 my_scenes 的增加，
    这样就可以结合 my_log 将任意时间节点的数据和场景恢复出来，并且能够运行。
    同时在执行 DDL 删除的时候，找到相关的 my_scenes 重新编译后，生成新的 scenes，
    同时存储到 my_scenes_log 中，以便于随时回溯。

    输入参数 ps_code: [{'ps_index': 0, 'ps_type': 'String'}, {'ps_index': 2, 'ps_type': 'String'}] 去掉换成专门记录参数的表

    DROP TABLE IF EXISTS my_scenes;
    DROP INDEX IF EXISTS scenes_group_id_idx;
    */
    CREATE TABLE IF NOT EXISTS my_scenes (
                    group_id BIGINT,
                    scenes_name VARCHAR(40),
                    sql_code VARCHAR,
                    --ps_code VARCHAR,
                    descrip VARCHAR,
                    is_batch BOOLEAN DEFAULT false,
                    PRIMARY KEY (scenes_name)
                    ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.db.MyScenesCache,cache_name=my_scenes,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    CREATE INDEX IF NOT EXISTS scenes_group_id_idx ON my_scenes (scenes_name, group_id);

    /**
    11、场景参数表：my_scenes_params
    DROP TABLE IF EXISTS my_scenes_params;
    DROP INDEX IF EXISTS scenes_params_idx;
    */
    CREATE TABLE IF NOT EXISTS my_scenes_params (
                    scenes_name VARCHAR,
                    ps_type VARCHAR,
                    ps_index INT,
                    PRIMARY KEY (scenes_name, ps_index)
                    ) WITH \"template=MyMeta_template,KEY_TYPE=cn.plus.model.db.MyScenesParamsPk,VALUE_TYPE=cn.plus.model.db.MyScenesParams,cache_name=my_scenes_params,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    -- 创建一个索引，便于检索数据
    CREATE INDEX IF NOT EXISTS scenes_params_idx ON my_scenes_params (scenes_name);

    /**
    13、记录场景操作的表：my_scenes_log
    DROP TABLE IF EXISTS my_scenes_log;
    DROP INDEX IF EXISTS my_scenes_log_create_date_idx;
    */
    CREATE TABLE IF NOT EXISTS my_scenes_log (
                    id BIGINT,
                    mycacheex VARBINARY,
                    create_date TIMESTAMP,
                    PRIMARY KEY (id)
                    ) WITH \"template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.MyScenesLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=my_scenes_log,cache_group=my_meta_log\";


    CREATE INDEX IF NOT EXISTS my_scenes_log_create_date_idx ON my_scenes_log (create_date DESC);

    /**
    14、将本用户组场景的使用权限，赋给其它用户组的表：
    将本用户组场景的使用权限，赋给其它用户组
    将 scenes_id 场景的使用权，提交给 to_group_id。因为并非是本用户组下面的，所以要使用函数
    实际上是把 to_group_id，替换为 group_id。这样使用方的用户组 id 为 to_group_id，替换为
    group_id ，那么 to_group_id 就有了使用 group_id 的权限了

    DROP TABLE IF EXISTS call_scenes;
    DROP INDEX IF EXISTS call_scenes_idx;
    */
    CREATE TABLE IF NOT EXISTS call_scenes (
                    id BIGINT,
                    group_id BIGINT,
                    to_group_id BIGINT,
                    scenes_name VARCHAR(40),
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.db.MyCallScenes,cache_name=call_scenes,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";
    CREATE INDEX IF NOT EXISTS call_scenes_idx ON call_scenes (to_group_id, scenes_name);

    /**
    15、查询的权限视图：my_select_views
    DROP TABLE IF EXISTS my_select_views;
    */
    CREATE TABLE IF NOT EXISTS my_select_views (
                    id BIGINT,
                    view_name VARCHAR(40),
                    table_name VARCHAR(40),
                    data_set_id BIGINT DEFAULT 0,
                    code VARCHAR,
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,cache_name=my_select_views,VALUE_TYPE=cn.plus.model.ddl.MySelectViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    16、更新的权限视图：my_update_views
    DROP TABLE IF EXISTS my_update_views;
    */
    CREATE TABLE IF NOT EXISTS my_update_views (
                    id BIGINT,
                    view_name VARCHAR(40),
                    table_name VARCHAR(40),
                    data_set_id BIGINT DEFAULT 0,
                    code VARCHAR,
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,cache_name=my_update_views,VALUE_TYPE=cn.plus.model.ddl.MyUpdateViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    17、修改的权限视图：my_insert_views
    DROP TABLE IF EXISTS my_insert_views;
    */
    CREATE TABLE IF NOT EXISTS my_insert_views (
                    id BIGINT,
                    view_name VARCHAR(40),
                    table_name VARCHAR(40),
                    data_set_id BIGINT DEFAULT 0,
                    code VARCHAR,
                    PRIMARY KEY (id)
                    ) WITH \"template=MyMeta_template,cache_name=my_insert_views,VALUE_TYPE=cn.plus.model.ddl.MyInsertViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    18、删除的权限视图：my_delete_views
    DROP TABLE IF EXISTS my_delete_views;
    */
    CREATE TABLE IF NOT EXISTS my_delete_views (
                    id BIGINT,
                    view_name VARCHAR(40),
                    table_name VARCHAR(40),
                    data_set_id BIGINT DEFAULT 0,
                    code VARCHAR,
                    PRIMARY KEY (id)
                ) WITH \"template=MyMeta_template,cache_name=my_delete_views,VALUE_TYPE=cn.plus.model.ddl.MyDeleteViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    19、用户组的权限视图：my_group_view
    1：增
    2：删
    3：改
    4：查
    DROP TABLE IF EXISTS my_group_view;
    */
    CREATE TABLE IF NOT EXISTS my_group_view (
                    id BIGINT,
                    my_group_id BIGINT,
                    view_id BIGINT,
                    view_type VARCHAR(2),
                    PRIMARY KEY (id, my_group_id)
                    ) WITH \"template=MyMeta_template,affinityKey=my_group_id,cache_name=my_group_view,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

    /**
    20、记录操作的表：my_log
    DROP TABLE IF EXISTS my_log;
    DROP INDEX IF EXISTS my_log_idx;
    */
    CREATE TABLE IF NOT EXISTS my_log (
                    id BIGINT,
                    table_name VARCHAR,
                    mycacheex VARBINARY,
                    create_date TIMESTAMP,
                    PRIMARY KEY (id)
                    ) WITH \"template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.MyLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=my_log,cache_group=my_meta_log\";

    CREATE INDEX IF NOT EXISTS my_log_idx ON my_log (table_name, create_date);

    /**
    21、记录DDL操作的表，作用是回溯复盘：ddl_log
    记录DDL操作的表，作用是回溯复盘
    DROP TABLE IF EXISTS ddl_log;
    DROP INDEX IF EXISTS ddl_log_group_id_idx;
    */
    CREATE TABLE IF NOT EXISTS ddl_log (
                    id BIGINT,
                    group_id BIGINT,
                    data_set_id BIGINT,
                    sql_code VARCHAR,
                    create_date TIMESTAMP,
                    PRIMARY KEY (id)
                    ) WITH \"template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.DdlLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=ddl_log,cache_group=my_meta_log\";
                    
    CREATE INDEX IF NOT EXISTS ddl_log_group_id_idx ON ddl_log (group_id, create_date);

    /**
    22、记录DDL操作数据集的语句，作用是回溯复盘：ddl_log
    记录DDL操作数据集的语句，作用是回溯复盘

    DROP TABLE IF EXISTS dataset_ddl_log;
    DROP INDEX IF EXISTS ds_ddl_log_idx;
    */
    CREATE TABLE IF NOT EXISTS dataset_ddl_log (
                    id BIGINT,
                    data_set_name VARCHAR,
                    ds_ddl_type VARCHAR,
                    sql_code VARCHAR,
                    create_date TIMESTAMP,
                    PRIMARY KEY (id)
                    ) WITH \"template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.DataSetDdlLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=dataset_ddl_log,cache_group=my_meta_log\";
                    
    CREATE INDEX IF NOT EXISTS ds_ddl_log_idx ON dataset_ddl_log (data_set_name, create_date);

    /**
    23、定时任务
    DROP TABLE IF EXISTS my_cron;
    */
    CREATE TABLE IF NOT EXISTS my_cron (
                      cron_name VARCHAR(40),
                      cron VARCHAR,
                      descrip VARCHAR,
                      PRIMARY KEY (cron_name)
                      ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.MyCron,cache_name=my_cron,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";

")

(def my-un-grid-tables
    ["DROP TABLE IF EXISTS my_users_group"
     "DROP TABLE IF EXISTS my_dataset"
     "DROP TABLE IF EXISTS my_dataset_table"
     "DROP TABLE IF EXISTS my_dataset_real_table"
     "DROP TABLE IF EXISTS my_meta_tables"
     "DROP TABLE IF EXISTS table_item"
     "DROP TABLE IF EXISTS table_index"
     "DROP TABLE IF EXISTS table_index_item"
     "DROP INDEX IF EXISTS my_meta_tables_idx"
     "DROP INDEX IF EXISTS table_item_idx"
     "DROP INDEX IF EXISTS table_index_idx"
     "DROP INDEX IF EXISTS ot_ds_tname_idx"
     "DROP TABLE IF EXISTS my_scenes"
     "DROP INDEX IF EXISTS scenes_group_id_idx"
     "DROP TABLE IF EXISTS my_scenes_params"
     "DROP INDEX IF EXISTS scenes_params_idx"
     "DROP TABLE IF EXISTS my_scenes_obj"
     "DROP INDEX IF EXISTS scenes_obj_tn_idx"
     "DROP INDEX IF EXISTS scenes_obj_in_idx"
     "DROP TABLE IF EXISTS my_scenes_log"
     "DROP INDEX IF EXISTS my_scenes_log_create_date_idx"
     "DROP TABLE IF EXISTS call_scenes"
     "DROP INDEX IF EXISTS call_scenes_idx"
     "DROP TABLE IF EXISTS my_select_views"
     "DROP TABLE IF EXISTS my_update_views"
     "DROP TABLE IF EXISTS my_insert_views"
     "DROP TABLE IF EXISTS my_delete_views"
     "DROP TABLE IF EXISTS my_group_view"
     "DROP TABLE IF EXISTS my_log"
     "DROP INDEX IF EXISTS my_log_idx"
     "DROP TABLE IF EXISTS ddl_log"
     "DROP INDEX IF EXISTS ddl_log_group_id_idx"
     "DROP TABLE IF EXISTS dataset_ddl_log"
     "DROP INDEX IF EXISTS ds_ddl_log_idx"
     ])

(def my-grid-tables-set #{"my_users_group"
                          "my_dataset"
                          "my_dataset_table"
                          "my_dataset_real_table"
                          "my_meta_tables"
                          "table_item"
                          "table_index"
                          "table_index_item"
                          "my_scenes"
                          "my_scenes_params"
                          "my_scenes_obj"
                          "my_scenes_log"
                          "call_scenes"
                          "my_select_views"
                          "my_update_views"
                          "my_insert_views"
                          "my_delete_views"
                          "my_group_view"
                          "my_log"
                          "ddl_log"
                          "dataset_ddl_log"})
