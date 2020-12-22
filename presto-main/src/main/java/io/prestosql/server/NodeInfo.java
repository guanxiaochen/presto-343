package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.metadata.NodeState;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class NodeInfo
{
    private final String identifier;
    private final String uri;
    private final String version;
    private final boolean coordinator;
    private final NodeState nodeState;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("identifier") String identifier,
            @JsonProperty("uri") String uri,
            @JsonProperty("version") String version,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("nodeState") NodeState nodeState)
    {
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.version = requireNonNull(version, "version is null");
        this.coordinator = coordinator;
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
    }

    @JsonProperty
    public String getIdentifier() {
        return identifier;
    }

    @JsonProperty
    public String getUri() {
        return uri;
    }

    @JsonProperty
    public String getVersion() {
        return version;
    }

    @JsonProperty
    public boolean isCoordinator() {
        return coordinator;
    }

    @JsonProperty
    public NodeState getNodeState() {
        return nodeState;
    }
}
