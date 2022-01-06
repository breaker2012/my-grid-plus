package org.gridgain.temp;

public class MyAdd {
    private Long count = 0L;
    private Long n = 0L;

    private TailRecursion<Long> my_num;

    public MyAdd(final Long n)
    {
        this.n = n;
    }

    private void add(final Long n, final Long nums)
    {
        if (n == 0L)
        {
            this.count = nums;
        }
        else
        {
            add(n - 1, n + nums);
        }
    }

    public Long recur_tail_add()
    {
        this.add(this.n, 0L);
        return this.count;
    }

    public Long recur_add(final Long n)
    {
        if (n == 0L)
            return 0L;
        else
            return n + recur_add(n - 1);
    }

    public TailRecursion<Long> recur(final Long n, final Long nums)
    {
        if (n == 0L)
        {
            return TailInvoke.done(nums);
        }
        else
        {
            //add(n - 1, n + nums);
            return TailInvoke.call(() -> recur(n - 1, n + nums));
        }
    }
}
