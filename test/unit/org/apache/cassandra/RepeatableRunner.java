/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;

/**
 * This class comes useful when debugging flaky tests that will fail only when the full test suite is ran. It is
 * intended for test failure investigation only. Decorate your class with the runner and hardcode the iterations you
 * need.
 * 
 * If your test uses a runner already, as they can't be chained, you can use this class as inspiration to modify the
 * original runner to loop your test.
 */
public class RepeatableRunner extends Runner
{
    private static final int REPETITIONS = Integer.parseInt(System.getProperty("cassandra.test.repetitions", "10"));

    private final Runner runner;

    private RepeatableRunner(Runner runner)
    {
        this.runner = runner;
    }

    public RepeatableRunner(Class<?> klass) throws InitializationError
    {
        this(new org.junit.runners.BlockJUnit4ClassRunner(klass));
    }

    @Override
    public Description getDescription()
    {
        return runner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier)
    {
        for (int i = 0; i < REPETITIONS; i++)
        {
            runner.run(notifier);
        }
    }

    public static class BlockJUnit4ClassRunner extends RepeatableRunner
    {
        public BlockJUnit4ClassRunner(Class<?> klass) throws InitializationError
        {
            super(new org.junit.runners.BlockJUnit4ClassRunner(klass));
        }
    }

    public static class Parameterized extends RepeatableRunner
    {
        public Parameterized(Class<?> klass) throws Throwable
        {
            super(new org.junit.runners.Parameterized(klass));
        }
    }

    public static class BMUnitRunner extends RepeatableRunner
    {
        public BMUnitRunner(Class<?> klass) throws Throwable
        {
            super(new org.jboss.byteman.contrib.bmunit.BMUnitRunner(klass));
        }
    }

    public static class OrderedJUnit4ClassRunner extends RepeatableRunner
    {
        public OrderedJUnit4ClassRunner(Class<?> klass) throws Throwable
        {
            super(new org.apache.cassandra.OrderedJUnit4ClassRunner(klass));
        }
    }

    public static class CassandraIsolatedJunit4ClassRunner extends RepeatableRunner
    {
        public CassandraIsolatedJunit4ClassRunner(Class<?> klass) throws Throwable
        {
            super(new org.apache.cassandra.CassandraIsolatedJunit4ClassRunner(klass));
        }
    }
}
