use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Error;
use Protocol::MQTT::Qos :qos;
use Protocol::MQTT::Message :decoder;

my %class-for-type = (
	0x1 => Message::Connect,
	0x2 => Message::ConnAck,
	0x3 => Message::Publish,
	0x4 => Message::PubAck,
	0x5 => Message::PubRec,
	0x6 => Message::PubRel,
	0x7 => Message::PubComp,
	0x8 => Message::Subscribe,
	0x9 => Message::SubAck,
	0xa => Message::Unsubscribe,
	0xb => Message::UnsubAck,
	0xc => Message::PingReq,
	0xd => Message::PingResp,
	0xe => Message::Disconnect,
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

	method get-packet(--> Message) {
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
