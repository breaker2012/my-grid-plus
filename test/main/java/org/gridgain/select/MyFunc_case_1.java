package org.gridgain.select;

import clojure.lang.LazySeq;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.plus.dml.MySelect;
import org.junit.Test;

public class MyFunc_case_1 {
    /**
     * 添加 func 到 cache
     * */
    @Test
    public void save_func() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        MySelect.putAstCache(ignite, "my_func", "func_1", "select distinct a.empno\n" +
                "                      from agent_info a, staff_info b\n" +
                "                     where (((c_rank_type = '03' and mgrno = c_empno) or\n" +
                "                           a.empno = c_empno) or\n" +
                "                           ((c_rank_type = '04' and chiefno = c_empno) or\n" +
                "                           a.empno = c_empno))\n" +
                "                       and ((a.end_date is null or a.end_date > p_sdate) and\n" +
                "                       a.start_date < p_edate + 1)\n" +
                "                       and ((b.leave_reson_no not in('07', '08', '09', '10', '12') and\n" +
                "                           b.leave_date > p_sdate) or b.leave_date is null)\n" +
                "                       and b.empno = a.empno\n" +
                "                       and substr(a.empno, 1, 4) not in ('1EC0', '1008')\n" +
                "                       and b.emp_name <> 'AUTOUNDW'\n" +
                "                    union\n" +
                "                    select distinct a.empno\n" +
                "                      from agent_info a, staff_info b\n" +
                "                     where (((c_rank_type = '03' and mgrno = c_empno) or\n" +
                "                           a.empno = c_empno) or\n" +
                "                           ((c_rank_type = '04' and chiefno = c_empno) or\n" +
                "                           a.empno = c_empno))\n" +
                "                       and ((a.end_date is null or a.end_date > p_sdate) and\n" +
                "                           a.start_date < p_edate + 1)\n" +
                "                       and b.empno = a.empno\n" +
                "                       and substr(a.empno, 1, 4) not in ('1EC0', '1008')\n" +
                "                       and b.emp_name <> 'AUTOUNDW'\n" +
                "                       and b.leave_reson_no in('07', '08', '09', '10', '12')\n" +
                "                       and b.leave_date >= add_months(p_sdate, 1)");
    }

    @Test
    public void read_func() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        LazySeq lazySeq = MySelect.getSqlToAst(ignite, "my_func", "func_1");
        System.out.println(lazySeq.first());
    }
}
