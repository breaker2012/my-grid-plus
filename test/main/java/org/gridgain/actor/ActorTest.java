package org.gridgain.actor;

import java.util.concurrent.CompletableFuture;

interface IActorA {
    void func();
}

interface IActorB {
    CompletableFuture<Integer> add(int a, int b);
}

final class ActorA extends ActorThread implements IActorA {
    private ActorA() {
        setName("ActorA");
    }

    @Override
    public void func() {
        System.out.println(Thread.currentThread().getName() + ": invoke add begin");
        getActor(IActorB.class).add(123, 456).thenAccept(r -> {
            System.out.println(Thread.currentThread().getName() + ": invoke add result = " + r);
            System.exit(0);
        });
        System.out.println(Thread.currentThread().getName() + ": invoke add end");
    }
}

final class ActorB extends ActorThread implements IActorB {
    private ActorB() {
        setName("ActorB");
    }

    @Override
    public CompletableFuture<Integer> add(int a, int b) {
        System.out.println(Thread.currentThread().getName() + ": in add");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(a + b);
    }
}

public final class ActorTest {
    /**
     * 一些说明:
     *
     * 1. 各Actor应该只公开接口, 而实现类不要暴露出来, 避免被误用.
     *
     * 2. 接口方法通常返回CompletableFuture类型,并在方法返回时返回 CompletableFuture.completedFuture(结果).
     *
     * 3. 如果没按2的方法实现返回, 那么调用方获得的future只能得到null结果.
     *
     * 4. 调用方得到future时很可能结果还没得到, 所以通常要用thenAccept等方法指定结果产生时的后续处理,或者继续并行执行其它代码.
     *
     * 5. 虽然接口方法能自动转到各自的Actor线程中运行, 但引用类型参数所引用的变量是共享的, 所以调用方和被调用方怎么访问参数(尤其是写操作)要好自为之. 在严格隔离的场合需要对参数做序列化和反序列化.
     * */
    public static void main(String[] args) throws Exception {
        ActorThread.init(ActorA.class);
        ActorThread.init(ActorB.class);
        System.out.println(Thread.currentThread().getName() + ": begin to func");
        ActorThread.getActor(IActorA.class).func();
    }
}
