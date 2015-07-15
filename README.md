# webscraper

Scrape the web up to a certain depth and store it as a graph.

## Usage Example

```sh
# if not already done:
mvn compile

# scrape the web up to depth 3 starting from http://www.tinkerpop.com and store the result in /tmp/web.kryo
./run /tmp/web.kryo http://www.tinkerpop.com 3

```
