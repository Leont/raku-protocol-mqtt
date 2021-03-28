use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Qos :qos;

our class Will is export {
	has Str:D  $.topic is required;
	has Str:D  $.message is required;
	has Bool:D $.retain = False;
	has Qos:D  $.qos = At-most-once;
}

