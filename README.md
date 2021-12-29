[![Actions Status](https://github.com/Leont/raku-protocol-mqtt/workflows/test/badge.svg)](https://github.com/Leont/raku-protocol-mqtt/actions)

NAME
====

Protocol::MQTT - blah blah blah

SYNOPSIS
========

```raku
use Protocol::MQTT;
```

DESCRIPTION
===========

Protocol::MQTT contains a networking and timing independent implementation of the MQTT protocol. Currently only the client side is implemented, in the form of [Protocol::MQTT::Client](Protocol::MQTT::Client). [Net::MQTT](Net::MQTT) an actual client based on `Protocol::MQTT::Client`.

AUTHOR
======

Leon Timmermans <fawaka@gmail.com>

COPYRIGHT AND LICENSE
=====================

Copyright 2021 Leon Timmermans

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

