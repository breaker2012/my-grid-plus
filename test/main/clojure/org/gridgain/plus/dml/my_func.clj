(ns org.gridgain.plus.dml.my-func
  (:require [clojure.test :refer :all]
            [org.gridgain.plus.dml.my-select :as my-select]
            [org.gridgain.plus.dml.select-lexical :as my-lexical]
            [org.gridgain.plus.context.my-context :as my-context]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [clojure.walk :as w])
  (:import (org.apache.ignite Ignite IgniteCache)
           (org.apache.ignite.internal IgnitionEx)
           (org.tools MyDbUtil MyTools)
           (org.apache.ignite.configuration CacheConfiguration)
           (org.apache.ignite.cache CacheMode CacheAtomicityMode)
           (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
           (org.gridgain.meta.cache MyContextCacheUtil)
           ))

(def ignite (IgnitionEx/start "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml"))

; 存储func 和 scenes 到数据库

; 存储 func 对象
(def sql_a "select * from table_a ta where ta.id > :a_id")
(def ps_a (my-select/->funcPs 0 ":a_id"))
(def func_a (my-select/->funcObj [ps_a] (my-select/sql-to-ast (my-lexical/to-back sql_a))))

(def sql_b "select * from table_b tb where tb.id > :b_id and tb.name = :b_name")
(def func_b (my-select/->funcObj [(my-select/->funcPs 0 ":b_id") (my-select/->funcPs 1 ":b_name")] (my-select/sql-to-ast (my-lexical/to-back sql_b))))

(def func_map {"func_a" func_a "func_b" func_b})

(def sql_scenes_a "select * from scenes_a ta where ta.id > :a_id")
(def scenes_a (my-select/->scenesObj [(my-select/->scenesPs 0 ":a_id")] (my-select/sql-to-ast (my-lexical/to-back sql_scenes_a))))

(def sql_scenes_b "select * from scenes_b tb where tb.id > :b_id and tb.name = :b_name")
(def scenes_b (my-select/->scenesObj [(my-select/->scenesPs 0 ":b_id") (my-select/->scenesPs 1 ":b_name")] (my-select/sql-to-ast (my-lexical/to-back sql_scenes_b))))

(def scenes_map {"scenes_a" scenes_a "scenes_b" scenes_b})

(defn put_func
  ([m ignite cacheName] (put_func (keys m) m ignite cacheName))
  ([[f & rs] m ignite cacheName]
   (if (some? f)
       (when-let [cache (my-select/get-cache ignite cacheName)]
           (do (.put cache f (get m f))
               (put_func rs m ignite cacheName))))))

; 保存 func
(put_func func_map ignite "my_meta_cache_all_func")
(put_func scenes_map ignite "my_meta_cache_all_scenes")

; 保存视图
; 查询视图 存储为 <table_name, select ast>
(def select_view "select emp_no, my_f(emp_name) from emp_table where emp_id > 1")
(def select_view_1 "select no, category from salary_table where s_id > 100")
(def view_map {:emp_table (my-select/sql-to-ast (my-lexical/to-back select_view))
               :salary_table (my-select/sql-to-ast (my-lexical/to-back select_view_1))})

(put_func view_map ignite "context_select_view")

; 1、测试替换 query items 中的 元素，并且判断 item 是否有权限
(def sql_query_items "select c.id, c.my_name, a.name from city c, student a where c.id = a.id")

(def sql_1 "select distinct a.empno,\n             round(nvl(sum(nvl(b.data,0)),0)*0.12,2)  as acc_dir_sal,\n             (select emp_name from staff_info where empno=a.empno) as emp_name,\n                          from agent_info a, staff_info b, (select emp_name from my_staff_info where empno=a.empno)\n                         where (((c_rank_type = '03' and mgrno = c_empno) or a.empno = c_empno) or\n             ((c_rank_type = '04' and chiefno = c_empno) or a.empno = c_empno))\n         and ((a.end_date is null or a.end_date > p_sdate ) and a.start_date <= p_edate)\n         and p_date between start_date and ((a.end_date is null or a.end_date > p_sdate ) and a.start_date <= p_edate)\n         and a.id exists (select 1\n                       from staff_info c, cs_prem_info b\n                      where b.busi_src = 'P'\n                        and b.j_date >= p_sdate\n                        and b.j_date < p_edate+1\n                        and b.s_flag = '01'\n                        and c.empno = b.agentno\n                        and substr(c.empno,1,4) not in ('1EC0','1008')\n                        and c.emp_name<>'AUTOUNDW'\n                        and ((b.PLAN_CODE = 'A57' and b.PREM_TIMES =1) or b.PLAN_CODE <> 'A57')\n                        --and cs_share_salary_package.func_is_accident_health_insur(b.plan_code) = 'N'\n                        and a.empno = c.empno\n                     having sum(b.tot_prem) >= 1000\n                   group by b.agentno)")

(my-select/sql-to-ast (my-lexical/to-back "select a.empno, round(nvl(sum(nvl(b.data,0)),0)*0.12,2) as acc_dir_sal, (select emp_name from staff_info where empno=a.empno) as emp_name from agent_info a, staff_info b where (((c_rank_type = '03' and mgrno = c_empno) or a.empno = c_empno) or ((c_rank_type = '04' and chiefno = c_empno) or a.empno = c_empno))"))

(my-select/sql-to-ast (my-lexical/to-back "select distinct a.empno, round(nvl(sum(nvl(b.data,0)),0)*0.12,2)  as acc_dir_sal, (select emp_name from staff_info where empno=a.empno) as emp_name from agent_info a, staff_info b, (select emp_name from my_staff_info where empno=a.empno) where (((c_rank_type = '03' and mgrno = c_empno) or a.empno = c_empno) or ((c_rank_type = '04' and chiefno = c_empno) or a.empno = c_empno))"))


(my-select/sql-to-ast (my-lexical/to-back "select distinct a.empno, func_a(m_c_t.id), round(nvl(sum(nvl(b.data,0)),0)*0.12,2)  as acc_dir_sal, (select emp_name from staff_info s, scenes_b(1, 2) as sb, city ct where ct.id > 10 and empno=a.empno and sb.id > 0) as emp_name from agent_info a, staff_info b, city m_c_t, (select emp_name from my_staff_info where empno=a.empno) where (((c_rank_type = '03' and mgrno = c_empno) or a.empno = c_empno) or ((c_rank_type = '04' and chiefno = c_empno) or a.empno = c_empno)) and ((a.end_date is null or a.end_date > p_sdate ) and a.start_date <= p_edate) and p_date between start_date and ((a.end_date is null or a.end_date > p_sdate ) and a.start_date <= p_edate) and a.id exists (select 1 from staff_info c, cs_prem_info b where b.busi_src = 'P' and b.j_date >= p_sdate and b.j_date < p_edate+1 and b.s_flag = '01' and c.empno = b.agentno and substr(c.empno,1,4) not in ('1EC0','1008') and c.emp_name<>'AUTOUNDW' and ((b.PLAN_CODE = 'A57' and b.PREM_TIMES =1) or b.PLAN_CODE <> 'A57') and a.empno = c.empno group by b.agentno having sum(b.tot_prem) >= 1000)"))
























































