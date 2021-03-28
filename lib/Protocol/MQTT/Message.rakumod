use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Qos :qos;

our class Message is export(:message) {
	has Str:D   $.topic is required;
	has blob8:D $.message is required;
	has Bool:D  $.retain = False;
	has Qos:D   $.qos = At-most-once;
}
