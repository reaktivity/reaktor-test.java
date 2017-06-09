/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.reaktor.test;

import static java.lang.String.valueOf;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static org.junit.runners.model.MultipleFailureException.assertEmpty;
import static org.reaktivity.nukleus.Configuration.COMMAND_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.DIRECTORY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.ReaktorBuilder;

public final class ReaktorRule implements TestRule
{
    private final Properties properties;
    private final ReaktorBuilder builder;

    private Reaktor reaktor;

    private Configuration configuration;
    private boolean clean;

    public ReaktorRule()
    {
        this.builder = Reaktor.builder();
        this.properties = new Properties();
    }

    public ReaktorRule directory(String directory)
    {
        properties.setProperty(DIRECTORY_PROPERTY_NAME, directory);
        return this;
    }

    public ReaktorRule commandBufferCapacity(int commandBufferCapacity)
    {
        properties.setProperty(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(commandBufferCapacity));
        return this;
    }

    public ReaktorRule responseBufferCapacity(int responseBufferCapacity)
    {
        properties.setProperty(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(responseBufferCapacity));
        return this;
    }

    public ReaktorRule counterValuesBufferCapacity(int counterValuesBufferCapacity)
    {
        properties.setProperty(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(counterValuesBufferCapacity));
        return this;
    }

    public ReaktorRule streamsBufferCapacity(int streamsBufferCapacity)
    {
        properties.setProperty(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(streamsBufferCapacity));
        return this;
    }

    public ReaktorRule throttleBufferCapacity(int throttleBufferCapacity)
    {
        properties.setProperty(THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME, valueOf(throttleBufferCapacity));
        return this;
    }

    public ReaktorRule clean()
    {
        this.clean = true;
        return this;
    }

    public ReaktorRule nukleus(
        Predicate<String> matcher)
    {
        builder.nukleus(matcher);
        return this;
    }

    public ReaktorRule controller(
        Predicate<Class<? extends Controller>> matcher)
    {
        builder.controller(matcher);
        return this;
    }

    public <T extends Nukleus> T nukleus(
        String name,
        Class<T> kind)
    {
        if (reaktor == null)
        {
            throw new IllegalStateException("Reaktor not started");
        }

        T nukleus = reaktor.nukleus(name, kind);
        if (nukleus == null)
        {
            throw new IllegalStateException("nukleus not found: " + name + " " + kind.getName());
        }

        return nukleus;
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        if (reaktor == null)
        {
            throw new IllegalStateException("Reaktor not started");
        }

        T controller = reaktor.controller(kind);
        if (controller == null)
        {
            throw new IllegalStateException("controller not found: " + kind.getName());
        }

        return controller;
    }

    private Configuration configuration()
    {
        if (configuration == null)
        {
            configuration = new Configuration(properties);
        }
        return configuration;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {
            private boolean shouldDeletePath(
                Path path)
            {
                final int count = path.getNameCount();
                return "control".equals(path.getName(count - 1).toString()) ||
                        (count >= 2 && "streams".equals(path.getName(count - 2).toString()));
            }

            @Override
            public void evaluate() throws Throwable
            {
                Configuration config = configuration();

                if (clean)
                {
                    Files.walk(config.directory(), FOLLOW_LINKS)
                         .filter(this::shouldDeletePath)
                         .map(Path::toFile)
                         .forEach(File::delete);
                }

                final List<Throwable> errors = new ArrayList<>();

                reaktor = builder.config(config)
                                 .errorHandler(errors::add)
                                 .build();

                try
                {
                    reaktor.start();

                    base.evaluate();
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
                finally
                {
                    try
                    {
                        reaktor.close();
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                    finally
                    {
                        assertEmpty(errors);
                    }
                }
            }
        };
    }
}
