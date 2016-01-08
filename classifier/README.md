## Description

### Overview
Classifier is a stand-alone M/R job which pre-process Crawler's output using Fast Classification Rules.
It works in the following way: all URLs are matched against a list of patterns, each with pre-assigned category. If it matches
to one or many of them, it is considered classified, corresponding content is discarded, and result is appended to the final result.
If it doesn't match, both url and its content appended to the intermediate output, which serves as an input for the next
classification stage.
