package io.prestosql.server;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.server.security.ResourceSecurity;
import io.prestosql.spi.QueryId;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

@Path("/v1/catalog")
public class CatalogResource
{
    private final ConnectorManager connectorManager;
    private final Announcer announcer;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            Announcer announcer)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog(CatalogInfo catalogInfo)
    {
        requireNonNull(catalogInfo, "catalogInfo is null");

        CatalogName connectorId = connectorManager.createCatalog(
                catalogInfo.getCatalogName(),
                catalogInfo.getConnectorName(),
                catalogInfo.getProperties());

        updateConnectorIdAnnouncement(announcer, connectorId);
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("{catalog}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog( @PathParam("catalog") String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        connectorManager.dropConnection(catalog);
        return Response.status(Response.Status.OK).build();
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName connectorId)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }
}
