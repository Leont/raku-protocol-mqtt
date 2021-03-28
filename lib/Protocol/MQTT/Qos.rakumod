use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

our enum Qos is export(:qos) (
	At-most-once  => 0x0,
	At-least-once => 0x1,
	Exactly-once  => 0x2,
);

