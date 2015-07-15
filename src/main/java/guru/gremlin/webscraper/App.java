package guru.gremlin.webscraper;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.gryo;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class App {

    final static SimpleDateFormat DURATION_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());

    public static void main(final String[] args) throws Exception {
        if (args.length < 2) {
            System.exit(2);
        }
        final String output = args[0];
        final String url = args[1];
        final Integer maxDepth = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        final long startTime = System.currentTimeMillis();
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        final WebScraper scraper = new WebScraper(maxDepth);
        graph.createIndex("url", Vertex.class);
        scraper.visit(g, url);
        File f = new File(output);
        if (!f.exists() || f.delete()) {
            FileOutputStream os = new FileOutputStream(output);
            graph.io(gryo()).writer().create().writeVertices(os, g.V(), Direction.BOTH);
            os.close();
        }
        final long duration = System.currentTimeMillis() - startTime;
        System.out.println("\nDuration: " + DURATION_FORMAT.format(new Date(duration - TimeZone.getDefault().getRawOffset())));
    }
}
