use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Qos :qos;
use Protocol::MQTT::Subsets;

our class Message is export(:message) {
	has Str:D   $.topic is required;
	has blob8:D $.payload is required;
	has Bool:D  $.retain = False;
	has Qos:D   $.qos = At-most-once;
}

=begin pod

=NAME Protocol::MQTT::Message

=head1 DESCRIPTION

C<Protocol::MQTT::Message> represents a complete message as sent to a channel.

=head1 ATTRIBUTES

=defn Str topic
The topic this message is sent to

=defn blob8 payload
The payload of this message

=defn Bool retain
Is this a retained message or not

=defn Protocol::MQTT::Qos qos
The Quality of Service

=end pod
