## Description

### Overview
Crawler fetches web content from selected URLs collected from the logs by external process. It utilizes Akka event
bus technology, where individual pieces interact independently using common event bus. Currently, it consists of
bootstrapper, input parser, cache builder, web harvester, and content flusher. URL can be processed either all together, or in
pre-deifned chunks - to save on memory consumption. Once content is collected for all URLs in a chunk, it is flushed to the output.
Cache builder interacts with external cache and
filter input list of URLs accordingly - only those which are new or not expired will be fetched from the web. Web harvester collects
content of filtered URL list and output it for Content Classifier. Content is stored as-is in UTF-8 encoding, except removing extra
spaces and non-printable characters.
