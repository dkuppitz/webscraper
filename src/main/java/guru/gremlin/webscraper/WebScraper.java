package guru.gremlin.webscraper;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class WebScraper {

    final static Pattern TITLE_PATTERN = Pattern.compile("(?<=<title>).*?(?=</title>)");
    final static Pattern HREF_PATTERN = Pattern.compile("(?<=a href=\")[^\"]*");
    final static Pattern ABSOLUTE_URL_PATTERN = Pattern.compile("^http(s)?://");
    final static Pattern BASE_URL_PATTERN = Pattern.compile("^http(s)?://[^/]*");
    final static Pattern REDIRECT_PATTERN = Pattern.compile("<meta.*http-equiv=\"refresh\".*url=([^\"]*)");

    final static Integer MAX_REDIRECTION_DEPTH = 10; // TODO: make that configurable?

    final private int maxDepth;

    private int redirectionDepth = 0;

    public WebScraper(final int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public void visit(final GraphTraversalSource g, final String url) {
        visit(g, null, null, url, this.maxDepth, null);
    }

    private void visit(final GraphTraversalSource g, final Vertex parent, final String relation, final String url,
                       final int depth, final Map<String, String> urlParams) {

        System.out.print(indent(depth) + url + " ... ");
        Optional<Vertex> existing = getSiteVertex(g, url, urlParams);
        if (existing.isPresent()) {
            System.out.println("already exists");
            Vertex ev = existing.get();
            if (parent != null) {
                final Optional<Edge> edge = g.V(parent).outE(relation).where(inV().is(ev)).tryNext();
                if (edge.isPresent()) {
                    final Edge ee = edge.get();
                    ee.property("weight", ee.<Integer>value("weight") + 1);
                } else {
                    parent.addEdge(relation, ev, "weight", 1);
                }
            }
            if (depth > 0) {
                g.V(existing).outE().sideEffect(t -> {
                    final Edge e = t.get();
                    final Vertex v = e.inVertex();
                    final String u = v.<String>value("url");
                    final Map<String, String> params = new HashMap<>();
                    v.properties().forEachRemaining(vp -> {
                        if (vp.key().startsWith("param.")) {
                            params.put(vp.key().substring("param.".length()), (String) vp.value());
                        }
                    });
                    if (e.label().equals("redirect")) {
                        this.redirectionDepth++;
                    } else {
                        this.redirectionDepth = 0;
                    }
                    if (this.redirectionDepth < MAX_REDIRECTION_DEPTH) {
                        visit(g, null, null, u, depth - 1, params);
                    }
                }).iterate();
            }
        } else {
            System.out.print("fetching data ... ");
            final String purl = parametrizeUrl(url, urlParams);
            final Vertex v;
            HttpURLConnection conn = null;
            int responseCode = 0;
            boolean broken = false;
            try {
                HttpURLConnection.setFollowRedirects(false);
                conn = (HttpURLConnection) new URL(purl).openConnection();
                conn.setInstanceFollowRedirects(false);
                conn.setRequestMethod("HEAD");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(30000);
                responseCode = conn.getResponseCode();
            } catch (Exception ignored) {
                broken = true;
            }
            broken |= responseCode >= 400;
            if (broken) {
                System.out.println("failed");
                this.redirectionDepth = 0;
                v = createSiteVertex(g, "broken-site", url, urlParams);
            } else {
                System.out.println("done");
                if (Arrays.asList(301, 302).indexOf(responseCode) >= 0) {
                    final String location = conn.getHeaderField("Location");
                    if (location != null) {
                        v = createSiteVertex(g, "redirect", url, urlParams);
                        final Map<String, String> params = new HashMap<>();
                        final String sanitizedLocation = sanitizeUrl(url, location, params);
                        if (parametrizeUrl(sanitizedLocation, params).equals(parametrizeUrl(url, urlParams))) {
                            v.addEdge("redirect", v, "weight", 1);
                        } else if (this.redirectionDepth < MAX_REDIRECTION_DEPTH) {
                            this.redirectionDepth++;
                            visit(g, v, "redirect", sanitizedLocation, depth, params);
                        }
                    } else {
                        this.redirectionDepth = 0;
                        broken = true;
                        v = createSiteVertex(g, "broken-redirect", url, urlParams);
                    }
                } else {
                    String contentType = conn.getContentType();
                    contentType = contentType != null ? contentType.split(";")[0] : null;
                    String content = "";
                    try {
                        if ("text/html".equals(contentType)) {
                            content = downloadContent(url);
                        }
                    } catch (Exception ignored) {
                    }
                    Matcher m = REDIRECT_PATTERN.matcher(content);
                    if (m.find()) {
                        v = createSiteVertex(g, "redirect", url, urlParams);
                        if (contentType != null) v.property("contentType", contentType);
                        final Map<String, String> params = new HashMap<>();
                        final String loc = sanitizeUrl(url, m.group(1), params);
                        this.redirectionDepth++;
                        if (this.redirectionDepth < MAX_REDIRECTION_DEPTH) {
                            visit(g, v, "redirect", loc, depth, params);
                        }
                    } else {
                        this.redirectionDepth = 0;
                        v = createSiteVertex(g, "site", url, urlParams);
                        if (contentType != null) v.property("contentType", contentType);
                        if ("text/html".equals(contentType)) {
                            m = TITLE_PATTERN.matcher(content);
                            if (m.find()) {
                                v.property("title", m.group());
                            }
                            if (depth > 0) {
                                m = HREF_PATTERN.matcher(content);
                                while (m.find()) {
                                    final Map<String, String> params = new HashMap<>();
                                    final String href = sanitizeUrl(url, m.group(), params);
                                    visit(g, v, "href", href, depth - 1, params);
                                }
                            }
                        }
                    }
                }
            }
            if (parent != null) {
                final String rel = broken ? ("broken-" + relation) : relation;
                final Optional<Edge> edge = g.V(parent).outE(rel).where(inV().is(v)).tryNext();
                if (edge.isPresent()) {
                    final Edge ee = edge.get();
                    ee.property("weight", ee.<Integer>value("weight") + 1);
                } else {
                    parent.addEdge(rel, v, "weight", 1);
                }
            }
        }
    }

    private String indent(final int depth) {
        final StringBuilder sb = new StringBuilder();
        for (int i = this.maxDepth; i > depth; i--) {
            sb.append("  ");
        }
        return sb.toString();
    }

    private Vertex createSiteVertex(final GraphTraversalSource g, final String type, final String url, final Map<String, String> params) {
        final Vertex v = g.addV(T.label, type, "url", url).next();
        if (params != null && !params.isEmpty()) {
            for (String key : params.keySet()) {
                v.property("param." + key, params.get(key));
            }
        }
        return v;
    }

    private String parametrizeUrl(final String url, final Map<String, String> params) {
        final StringBuilder sb = new StringBuilder(url);
        if (params != null && !params.isEmpty()) {
            boolean isFirst = true;
            for (final String key : params.keySet().stream().sorted().collect(Collectors.toList())) {
                sb.append(isFirst ? "?" : "&").append(key).append("=").append(params.get(key));
                isFirst = false;
            }
        }
        return sb.toString();
    }

    private Optional<Vertex> getSiteVertex(final GraphTraversalSource g, final String url, final Map<String, String> urlParams) {
        GraphTraversal<Vertex, Vertex> traversal = g.V().has("url", url);
        if (urlParams != null && !urlParams.isEmpty()) {
            for (String key : urlParams.keySet().stream().sorted().collect(Collectors.toList())) {
                traversal = traversal.has("param." + key, urlParams.get(key));
            }
        }
        return traversal.tryNext();
    }

    private String sanitizeUrl(String url, String href, final Map<String, String> params) {
        int i;
        try {
            href = URLDecoder.decode(href, "UTF-8");
        } catch (Exception ignored) {
        }
        if ((i = href.indexOf("#")) >= 0) href = href.substring(0, i);
        if ((i = href.indexOf(";jsessionid=")) >= 0) href = href.substring(0, i);
        if ((i = href.indexOf("?")) >= 0) {
            String[] parts = href.split("\\?", 2);
            href = parts[0];
            for (String pair : parts[1].split("&")) {
                String kv[] = pair.split("=", 2);
                String key = kv[0];
                String value = kv.length == 2 ? kv[1] : "";
                params.put(key, value);
            }
            href = href.substring(0, i);
        }
        if (!ABSOLUTE_URL_PATTERN.matcher(href).find()) {
            if (href.startsWith("/")) {
                final Matcher m2 = BASE_URL_PATTERN.matcher(url);
                if (m2.find()) href = m2.group() + href;
            } else {
                if (!url.endsWith("/")) {
                    if (url.substring(url.indexOf("://") + 3).indexOf("/") > 0) {
                        url = url.substring(0, url.lastIndexOf("/") + 1);
                    } else {
                        url = url + "/";
                    }
                }
                href = url + href;
            }
        }
        try {
            return new URI(href).normalize().toURL().toString();
        } catch (Exception ignored) {
            return href;
        }
    }

    private String downloadContent(String url) throws Exception {
        try {
            URLConnection conn = new URL(url).openConnection();
            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            br.close();
            return sb.toString();
        } catch (IOException ex) {
            return "";
        }
    }
}
