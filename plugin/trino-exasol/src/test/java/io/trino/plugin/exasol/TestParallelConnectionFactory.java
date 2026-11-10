/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exasol;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

final class TestParallelConnectionFactory
{
    @Test
    void testParsesDriverLoggingOptions()
    {
        Properties properties = ParallelConnectionFactory.parseJdbcLoggingProperties("jdbc:exa:exa.example.com:8563;schema=tpch;debug=1;logdir=/var/log/exasol jdbc");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(properties).containsOnlyKeys("debug", "logdir");
        softly.assertThat(properties.getProperty("debug")).isEqualTo("1");
        softly.assertThat(properties.getProperty("logdir")).isEqualTo("/var/log/exasol jdbc");
        softly.assertAll();
    }

    @Test
    void testNoDriverLoggingOptions()
    {
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(ParallelConnectionFactory.parseJdbcLoggingProperties("jdbc:exa:exa.example.com:8563;schema=tpch")).isEmpty();
        softly.assertThat(ParallelConnectionFactory.parseJdbcLoggingProperties("jdbc:exa:exa.example.com:8563;debug=0;logdir=/var/log/exasol")).isEmpty();
        softly.assertAll();
    }

    @Test
    void testLoggingWithoutLogDirectory()
    {
        Properties properties = ParallelConnectionFactory.parseJdbcLoggingProperties("jdbc:exa:exa.example.com:8563;debug=1");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(properties).containsOnlyKeys("debug");
        softly.assertThat(properties.getProperty("debug")).isEqualTo("1");
        softly.assertAll();
    }

    @Test
    void testWorkerPropertiesIncludeDriverLoggingOptions()
    {
        Properties loggingProperties = ParallelConnectionFactory.parseJdbcLoggingProperties("jdbc:exa:exa.example.com:8563;debug=1;logdir=/var/log/exasol");
        Properties workerProperties = ParallelConnectionFactory.buildConnectionProperties(123, 456, "FINGERPRINT", loggingProperties);

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(workerProperties).containsOnlyKeys("workertoken", "sessionid", "autocommit", "fingerprint", "validateservercertificate", "debug", "logdir");
        softly.assertThat(workerProperties.getProperty("workertoken")).isEqualTo("123");
        softly.assertThat(workerProperties.getProperty("sessionid")).isEqualTo("456");
        softly.assertThat(workerProperties.getProperty("autocommit")).isEqualTo("0");
        softly.assertThat(workerProperties.getProperty("fingerprint")).isEqualTo("FINGERPRINT");
        softly.assertThat(workerProperties.getProperty("validateservercertificate")).isEqualTo("1");
        softly.assertThat(workerProperties.getProperty("debug")).isEqualTo("1");
        softly.assertThat(workerProperties.getProperty("logdir")).isEqualTo("/var/log/exasol");
        softly.assertAll();
    }

    @Test
    void testWorkerPropertiesWithoutDriverLoggingOptions()
    {
        Properties workerProperties = ParallelConnectionFactory.buildConnectionProperties(123, 456, "FINGERPRINT", new Properties());

        assertThat(workerProperties).containsOnlyKeys("workertoken", "sessionid", "autocommit", "fingerprint", "validateservercertificate");
    }
}
