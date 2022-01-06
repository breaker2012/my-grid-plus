/**
 * 在内存中生成一个 hash-map 记录用户组的上下文
 * 数据结构为 PersistentHashMap 对应于 clojure 就是 hash-map

 例如：表示 group id 为 1 的 
{1 {"data_set_table" #{"table_a"},
    "data_set" "test_data_set",
    "update_views" {},
    "func_views" {},
    "scenes_views" {},
    "select_views" {"city" "select dafu(m.ID), m.District, m.Name from City as m where m.ID = 555"},
    "insert_views" {},
    "delete_views" {}}}
*/
user_context = {
    // userGroupId 用户索引上下文
    userGroupId: 1,
    context: {
        data_set: 'my_data_set',
        // data_set_table 的数据结构为 PersistentHashSet 对应于 clojure 就是 set
        data_set_table: {table_name},
        select_views: {table_name: '表名', ast: 'ast 语法树'},
        update_views: {table_name: '表名', ast: 'ast 语法树'},
        insert_views: {table_name: '表名', ast: 'ast 语法树'},
        delete_views: {table_name: '表名', ast: 'ast 语法树'},
        scenes_views: {scenes_name: '场景名', ast: 'ast 语法树'},
        func_views: {func_name: '自定义函数名', ast: 'ast 语法树'}
    }
};

/**
 * 场景上下文
*/
scenes-context = {
    scenes_name: "dagui",
    input_paras: [{
            index: 1,
            parameter_name: "a",
            parameter_type: "",
            parameter_java_type: "String",
            parameter_value: ''
        }, {
            index: 1,
            parameter_name: "b",
            parameter_type: "",
            parameter_java_type: "Integer",
            parameter_value: ''
        }, {
            index: 1,
            parameter_name: "c",
            parameter_type: "",
            parameter_java_type: "String",
            parameter_value: ''
        }],
        nodes: [{
            seq_name: "dagui",
            sql: "SELECT m.CategoryName as CategoryName, m.Description as Description FROM categories AS m WHERE m.CategoryID = 12"
        }, {
            seq_name: "wudafu",
            sql: "SELECT m.CompanyName as CompanyName, m.ContactName as ContactName FROM customers AS m WHERE m.CustomerID = 10"
        }]
};

/**
 * 
 * 记录场景资源

my_scenes_table 中用到的表
my_scenes_ss 中用到的场景
my_scenes_func 中用到的场景

这个的作用是当 view 被修改，删除的时候，先查询 scenes 使用的资源，同步更新场景所用到的是 view。
因为修改权限视图，不会那么频繁，所以直接把权限视图编译进场景是较优选择！

还要同步修改编译的数据结构
*/