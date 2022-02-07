use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

=begin pod

=head1 NAME

Protocol::MQTT -  A (sans-io) MQTT client implementation

=head1 SYNOPSIS

=begin code :lang<raku>

use Protocol::MQTT;

=end code

=head1 DESCRIPTION

Protocol::MQTT contains a networking and timing independent implementation of the MQTT protocol. Currently only the client side is implemented, in the form of L<Protocol::MQTT::Client|Protocol::MQTT::Client>. L<Net::MQTT|Net::MQTT> an actual client based on C<Protocol::MQTT::Client>.

=head1 AUTHOR

Leon Timmermans <fawaka@gmail.com>

=head1 COPYRIGHT AND LICENSE

Copyright 2021 Leon Timmermans

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
