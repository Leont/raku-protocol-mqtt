use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

our enum Qos is export(:qos) (
	At-most-once  => 0x0,
	At-least-once => 0x1,
	Exactly-once  => 0x2,
);

=begin pod

=NAME Protocol::MQTT::Qos

=head1 DESCRIPTION

This defines an enum with three possible values:

=defn C<At-most-once>
Messages are delivered according to the best efforts of the operating environment. Message loss can occur. This level could be used, for example, with ambient sensor data where it does not matter if an individual reading is lost as the next one will be published soon after.

=defn C<At-least-once>
Messages are assured to arrive but duplicates can occur.

=defn C<Exactly-once>
Message are assured to arrive exactly once. This level could be used, for example, with billing systems where duplicate or lost messages could lead to incorrect charges being applied.

=end pod
