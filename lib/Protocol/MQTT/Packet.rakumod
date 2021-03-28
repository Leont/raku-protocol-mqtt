use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Error;
use Protocol::MQTT::Message :message;
use Protocol::MQTT::Subsets;
use Protocol::MQTT::Qos :qos;

my enum Type (
	Connect     => 0x1,
	ConnAck     => 0x2,
	Publish     => 0x3,
	PubAck      => 0x4,
	PubRec      => 0x5,
	PubRel      => 0x6,
	PubComp     => 0x7,
	Subscribe   => 0x8,
	SubAck      => 0x9,
	Unsubscribe => 0xa,
	UnsubAck    => 0xb,
	PingReq     => 0xc,
	PingResp    => 0xd,
	Disconnect  => 0xe,
);

my role Skip[Int $count = 1] {
	method count() {
		return $count;
	}
}

my sub unpack-flags(Int $input, *@selectors) {
	my $index = 0;
	my @result;
	for @selectors {
		when Bool {
			@result.push(?($input +& (1 +< $index)));
			$index++;
		}
		when Qos {
			my $mask = 3 +< $index;
			@result.push: Qos(($input +& $mask) +> $index) // die Error::InvalidValue.new('Invalid qos');
			$index += 2;
		}
		when Skip {
			$index += .count;
		}
	}
	return @result;
}

our class DecodeBuffer is export(:decoder) {
	has buf8:D $!buffer is required;
	has Int:D $!offset = 0;
	submethod BUILD(:$!buffer) {
	}

	method decode-byte(--> Byte) {
		die Error::InsufficientData.new('decode-byte') unless $!buffer.elems >= $!offset + 1;
		my $result = $!buffer.read-uint8($!offset);
		$!offset++;
		return $result;
	}

	method unpack-byte(*@selectors) {
		return unpack-flags(self.decode-byte, |@selectors);
	}

	method decode-short(--> Short) {
		die Error::InsufficientData.new('decode-short') unless $!buffer.elems >= $!offset + 2;
		my $result = $!buffer.read-uint16($!offset, Endian::BigEndian);
		$!offset += 2;
		return $result;
	}

	method !decode-variable-length(Str $name) {
		my $len = self.decode-short;
		die Error::InsufficientData.new("decode-$name") unless $!buffer.elems >= $!offset + $len;
		my buf8 $result = $!buffer.subbuf($!offset, $len);
		$!offset += $len;
		return $result;
	}

	method decode-buffer(--> Buf) {
		return self!decode-variable-length('buffer');
	}

	method decode-string(--> Str) {
		return self!decode-variable-length('string').decode('utf8-c8');
	}

	method has-more(--> Bool) {
		return $!offset < $!buffer.elems;
	}

	method rest(--> Buf) {
		my buf8 $result = $!buffer.subbuf($!offset);
		$!offset = $!buffer.elems;
		return $result;
	}
}

my class EncodeBuffer {
	has buf8 $!buffer = buf8.new;
	has Int $!offset = 0;

	method encode-byte(Byte $byte) {
		$!buffer.write-uint8($!offset, $byte);
		$!offset++;
	}

	method encode-short(Short $short) {
		$!buffer.write-uint16($!offset, $short, Endian::BigEndian);
		$!offset += 2;
	}

	method encode-blob(Blob $blob) {
		self.encode-short($blob.bytes);
		$!buffer.append($blob);
		$!offset += $blob.bytes;
	}

	method encode-string(Str $string) {
		self.encode-blob($string.encode('utf8'));
	}

	method append-buffer(Blob $blob) {
		$!buffer.append($blob);
	}

	my sub encode-length(Int $length is copy --> buf8) {
		my $buffer = buf8.new;
		repeat {
			my $current-byte = $length % 128;
			$length div= 128;
			$current-byte +|= 0x80 if $length;
			$buffer.write-uint8($buffer.elems, $current-byte);
		} while $length;
		return $buffer;
	}

	method Buf(--> Buf) {
		return encode-length($!buffer.elems) ~ $!buffer;
	}
}

my sub pack-flags(*@values) {
	my $flag = 0;
	my $index = 0;
	for @values -> $value {
		when $value ~~ Bool {
			$flag +|= $value +< $index;
			$index++;
		}
		when $value ~~ Qos {
			$flag +|= $value +< $index;
			$index += 2;
		}
		when $value ~~ Type {
			$flag +|= $value +< $index;
			$index += 4;
		}
		when $value ~~ Skip {
			$index += $value.count;
		}
	}
	return $flag;
}

our role Packet[Type $type, Qos $qos = At-most-once] is export(:packets :decoder) {
	method message-type(--> Type) {
		return $type;
	}
	method header-byte(--> Byte) {
		return pack-flags(False, $qos, False, $type);
	}
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $flags --> Packet:D) {
		...
	}
	method !encode-body(Packet:D: EncodeBuffer --> Nil) {
		...
	}

	method encode(Packet:D: --> Buf) {
		my $buffer = EncodeBuffer.new;
		self!encode-body($buffer);
		return buf8.new(self.header-byte) ~ $buffer.Buf;
	}
}

my role Packet::Empty {
	method decode-body(Packet:U: DecodeBuffer $, Int $ --> Packet) {
		return self.new;
	}
	method !encode-body(Packet:D: EncodeBuffer $ --> Nil) {
	}
}

my role Packet::JustId {
	has Short:D $.packet-id is required;
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $ --> Packet) {
		return self.new(:packet-id($buffer.decode-short));
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
	}
}

our class Packet::Connect does Packet[Type::Connect] is export(:packets) {
	has Str:D $.protocol-name = 'MQTT';
	has Byte:D $.protocol-version = 4;

	has Bool:D $.clean-start = True;

	has Short:D $.keep-alive-interval = 0;
	has Str:D $.client-identifier is required;

	has Message $.will;

	has Str $.username;
	has Blob $.password;

	method decode-body(DecodeBuffer $buffer, Int $  --> Packet::Connect) {
		my $protocol-name = $buffer.decode-string;
		my $protocol-version = $buffer.decode-byte;

		my ($clean-start, $will-flag, $qos, $retain, $password-flag, $username-flag) = $buffer.unpack-byte(Skip, Bool, Bool, Qos, Bool, Bool, Bool);

		my %args = (:$protocol-name, :$protocol-version, :$clean-start);

		%args<keep-alive-interval> = $buffer.decode-short;
		%args<client-identifier> = $buffer.decode-string;

		if $will-flag {
			my $topic = $buffer.decode-string;
			my $message = $buffer.decode-buffer;
			%args<will> = Message.new(:$topic, :$message, :$qos, :$retain);
		}
		if $username-flag {
			%args<username> = $buffer.decode-string;
		}
		if $password-flag {
			%args<password> = $buffer.decode-buffer;
		}

		return self.new(|%args);
	}

	method !encode-body(Packet::Connect:D: EncodeBuffer $buffer) {
		$buffer.encode-string($!protocol-name);
		$buffer.encode-byte($!protocol-version);
		$buffer.encode-byte(pack-flags(Skip, $!clean-start, ?$!will, $!will ?? $!will.qos !! At-most-once, $!will ?? $!will.retain !! False, $!password.defined, $!username.defined));
		$buffer.encode-short($!keep-alive-interval);
		$buffer.encode-string($!client-identifier);
		with $!will {
			$buffer.encode-string($!will.topic);
			$buffer.encode-blob($!will.message);
		}
		$buffer.encode-string($!username) with $!username;
		$buffer.encode-blob($!password) with $!password;
	}
}

our class Packet::ConnAck does Packet[Type::ConnAck] is export(:packets) {
	our enum ConnectStatus (
		Accepted                              => 0,
		Refused-unacceptable-protocol-version => 1,
		Refused-identifier-rejected           => 2,
		Refused-server-unavailable            => 3,
		Refused-bad-user-name-or-password     => 4,
		Refused-not-authorized                => 5,
	);

	has Bool:D $.session-acknowledge = False;
	has ConnectStatus:D $.return-code = Accepted;

	method success(--> Bool) {
		return $!return-code === Accepted;
	}

	method decode-body(Packet::ConnAck:U: DecodeBuffer $buffer, Int $) {
		my ($session-acknowledge) = $buffer.unpack-byte(Bool);
		my $return-code = ConnectStatus($buffer.decode-byte) orelse die Error::InvalidValue.new('Invalid connect status');
		return self.new(:$session-acknowledge, :$return-code);
	}

	method !encode-body(Packet::ConnAck:D: EncodeBuffer $buffer) {
		$buffer.encode-byte(pack-flags($.session-acknowledge));
		$buffer.encode-byte(+$.return-code);
	}
}

class Packet::Publish does Packet[Type::Publish] is export(:packets) {
	has Qos:D $.qos = At-least-once;
	has Bool:D $.retain = False;
	has Bool:D $.dup = False;

	has Str:D $.topic is required;
	has Short $.packet-id;
	has Blob:D $.message is required;

	submethod TWEAK(:$!qos = At-most-once) {
		die Error::Semantic.new('') if $!topic !~~ Topic;
		die Error::Semantic.new('No packet-id on publish with qos') if $!qos > At-most-once && !$!packet-id.defined;
		die Error::Semantic.new('Can\'t duplicate qos-less message') if $!qos == At-most-once && $!dup;
	}
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $flags --> Packet) {
		my ($retain, $qos, $dup) = unpack-flags($flags, Bool, Qos, Bool);

		my $topic = $buffer.decode-string;
		my $packet-id = $qos ?? $buffer.decode-short !! Short;
		my $message = $buffer.rest;

		return self.new(:$dup, :$qos, :$retain, :$topic, :$packet-id, :$message);
	}
	method header-byte(--> Byte) {
		return pack-flags($!retain, $!qos, $!dup, Type::Publish);
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer --> Nil) {
		$buffer.encode-string($!topic);
		$buffer.encode-short($!packet-id) if $!qos;
		$buffer.append-buffer($!message);
	}
}

our class Packet::PubAck does Packet[Type::PubAck] does Packet::JustId is export(:packets) {
}

our class Packet::PubRec does Packet[Type::PubRec] does Packet::JustId  is export(:packets) {
}

our class Packet::PubRel does Packet[Type::PubRel, At-least-once] does Packet::JustId is export(:packets) {
}

our class Packet::PubComp does Packet[Type::PubComp] does Packet::JustId is export(:packets) {
}

our class Packet::Subscribe does Packet[Type::Subscribe, At-least-once] does Packet::JustId is export(:packets) {
	class Subscription {
		has Str:D $.topic is required;
		has Qos:D $.qos is required;
	}

	has Subscription @.subscriptions is required;
	submethod TWEAK() {
		Error::Semantic.new('Subscribe without subscriptions is invalid') if not @!subscriptions;
	}
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @subscriptions;
		while $buffer.has-more {
			my $topic = $buffer.decode-string;
			my ($qos) = $buffer.unpack-byte(Qos);
			@subscriptions.push: Subscription.new(:$topic, :$qos);
		}
		return self.new(:$packet-id, :@subscriptions);
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!subscriptions -> $subscription {
			$buffer.encode-string($subscription.topic);
			$buffer.encode-byte(pack-flags($subscription.qos));
		}
	}
}

our class Packet::SubAck does Packet[Type::SubAck] does Packet::JustId is export(:packets) {
	has Qos:D @.qos-levels;
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @qos-levels;
		while $buffer.has-more {
			@qos-levels.append: $buffer.unpack-byte(Qos);
		}
		return self.new(:$packet-id, :@qos-levels);
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!qos-levels -> $qos-level {
			$buffer.encode-byte(+$qos-level);
		}
	}
}

our class Packet::Unsubscribe does Packet[Type::Unsubscribe, At-least-once] does Packet::JustId is export(:packets) {
	has Str @.subscriptions;

	method decode-body(Packet:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @subscriptions;
		while $buffer.has-more {
			@subscriptions.push: $buffer.decode-string;
		}
		return self.new(:$packet-id, :@subscriptions);
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!subscriptions -> $subscription {
			$buffer.encode-string($subscription);
		}
	}
}

our class Packet::UnsubAck does Packet[Type::UnsubAck] does Packet::JustId is export(:packets) {
}

our class Packet::PingReq does Packet[Type::PingReq] does Packet::Empty is export(:packets) {
}

our class Packet::PingResp does Packet[Type::PingResp] does Packet::Empty is export(:packets) {
}

our class Packet::Disconnect does Packet[Type::Disconnect] does Packet::Empty is export(:packets) {
}

