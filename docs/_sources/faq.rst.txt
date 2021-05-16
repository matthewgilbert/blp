FAQ
====

How do I do X with the Bloomberg API?
#####################################

The best place to get info on using the ``blpapi`` API provided by Bloomberg is the official documentation. This can be
accessed from a  Bloomberg Terminal using ``WAPI <GO>` -> `API Developer's Guide``. You can also access the documentation
`here <https://www.bloomberg.com/professional/support/api-library/>`_.

How do I use ``isin/cusip/sedol``?
###################################

The default for Bloomberg is to use ``/ticker``. To change this you can explicitly specify the topic,
e.g. ``/cusip/097023105``. Documentation on topics can be found in Section 2.3 of the Bloomberg Core Developer's Guide.
The relevant section is included below:

    Topic: In the case of “//blp/mktdata,” the topic value consists of an optional topic prefix followed by an instrument
    identifier. For example, “/cusip/097023105” and “/sedol1/2108601” include the topic prefix, whereas “IBM US
    Equity” omits the topic prefix. If the topic prefix is not specified, the defaultTopicPrefix of the SessionOptions object is
    used, which is “/ticker” by default. Therefore, if using a ticker, such as IBM, the security string would be “IBM US
    Equity,” with the “/ticker” topic prefix is implied. Note: The topic’s form may be different for different subscription
    services.
