package io.trino.plugin.exasol;

public record ExasolWorkerNode(int id, String host, long token, long sessionId)
{
    public String getConnectionUrl()
    {
        // TODO: Add additional options from original JDBC URL
        //return "jdbc:exa-worker:%s:%d;workertoken=%d;worker=1;autocommit=0;encryption=1".formatted(host, port, token);
        return "jdbc:exa-worker:%s;workertoken=%d;sessionid=%d;worker=1;autocommit=0;encryption=1,validateservercertificate=0;fingerprint=5C9BC18A4263243019C6B72504707597A8503764511321550401B212588FF948".formatted(host, token, sessionId);
    }
}
