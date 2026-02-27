package io.trino.plugin.exasol;

import com.exasol.containers.ExasolContainer;

import java.util.stream.IntStream;

// TODO: Dynamically map ports in ExasolWorkerNodeConnectionFactory
class FixedHostPortExasolContainer<T extends FixedHostPortExasolContainer<T>> extends ExasolContainer<T>
{
    FixedHostPortExasolContainer(String dockerImageName) {
        super(dockerImageName);
    }

    FixedHostPortExasolContainer<T> withFixedExposedWorkerNodePorts()
    {
        // Worker node ports for parallel connections, see https://docs.exasol.com/db/latest/administration/on-premise/manage_network/system_network_settings.htm#Defaultports
        IntStream.range(20000, 21001).forEach(port -> {
            addFixedExposedPort(port, port);
        });
        return this;
    }
}
