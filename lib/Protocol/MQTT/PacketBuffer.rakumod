use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Error;
use Protocol::MQTT::Packet :decoder;
use Protocol::MQTT::Qos :qos;

my %class-for-type = (
	0x1 => Packet::Connect,
	0x2 => Packet::ConnAck,
	0x3 => Packet::Publish,
	0x4 => Packet::PubAck,
	0x5 => Packet::PubRec,
	0x6 => Packet::PubRel,
	0x7 => Packet::PubComp,
	0x8 => Packet::Subscribe,
	0x9 => Packet::SubAck,
	0xa => Packet::Unsubscribe,
	0xb => Packet::UnsubAck,
	0xc => Packet::PingReq,
	0xd => Packet::PingResp,
	0xe => Packet::Disconnect,
);

our class PacketBuffer is export(:decoder) {
	has buf8:D $!buffer is required;
	submethod BUILD(buf8:D :$!buffer = buf8.new) {}

	method add-data(Blob[uint8] $data --> Nil) {
		$!buffer.append($data);
	}

	my sub decode-length(buf8 $buffer, Int $offset is rw --> Int) {
		my $multiplier = 1;
		my $length = 0;
		loop {
			return Nil if $offset >= $buffer.elems;
			my $byte = $buffer.read-uint8($offset);
			$offset++;
			$length += ($byte +& 0x7f) * $multiplier;
			$multiplier *= 128;
			last unless $byte +& 0x80;
		}

		return $length;
	}

	method has-packet(--> Bool) {
		return False if $!buffer.elems < 2;
		my $offset = 1;
		my $remaining = decode-length($!buffer, $offset) orelse return False;
		return $!buffer.elems >= $offset + $remaining;
	}

	method get-packet(--> Packet) {
		return Nil if $!buffer.elems < 2;

		my $byte1 = $!buffer.read-uint8(0);
		my $offset = 1;
		my $remaining = decode-length($!buffer, $offset) orelse return Nil;

		if $!buffer.elems >= $offset + $remaining {
			my $buffer = $!buffer.subbuf($offset, $remaining);
			$!buffer.=subbuf($offset + $remaining);

			my $packet-type = $byte1 +> 4;
			die Error::InvalidValue.new('Invalid MQTT type') unless %class-for-type{$packet-type}:exists;
			my $type = %class-for-type{$packet-type};

			my $flags = $byte1 +& 0xF;
			my $decoder = DecodeBuffer.new(:$buffer);
			return $type.decode-body($decoder, $flags);
		}
		else {
			return Nil;
		}
	}
}
