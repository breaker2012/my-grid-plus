package org.gridgain.clojure;

import clojure.lang.Compiler;
import clojure.lang.RT;
import org.junit.Test;

import java.io.StringReader;

public class MyClojure {

    @Test
    public void test_1()
    {
        RT.init();
        Compiler.load(new StringReader("(eval )"));
    }
}
