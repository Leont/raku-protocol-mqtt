use v6.d;

unit module Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

our class MqttError is Exception is export(:exceptions :types) {
}

our class SemanticError is MqttError is export(:exceptions :types) {
	has Str $.message;
	method new($message) {
		return self.bless(:$message);
	}
}

our class DecodeError is MqttError is export(:exceptions :types) {
}

our class InsufficientData is DecodeError is export(:exceptions :types) {
	has Str $.where;
	method new(Str $where) {
		return self.bless(:$where);
	}
	method message(--> Str) {
		return "$!where: insufficient data";
	}
}

our class InvalidValue is DecodeError is export(:exceptions :types) {
	has Str:D $.message is required;
	method new(Str $message = 'Invalid value') {
		return self.bless(:$message);
	}
}

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

our enum Qos is export(:enum :qos :types) (
	At-most-once  => 0x0,
	At-least-once => 0x1,
	Exactly-once  => 0x2,
);

our enum ConnectStatus is export(:enum :connect-status :types) (
	Accepted                              => 0,
	Refused-unacceptable-protocol-version => 1,
	Refused-identifier-rejected           => 2,
	Refused-server-unavailable            => 3,
	Refused-bad-user-name-or-password     => 4,
	Refused-not-authorized                => 5,
);

subset Byte is export(:subsets :types) of Int where 0 .. 255;
subset Short is export(:subsets :types) of Int where 0 .. 65535;

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
			@result.push: Qos(($input +& $mask) +> $index) // die InvalidValue.new('Invalid qos');
			$index += 2;
		}
		when Skip {
			$index += .count;
		}
	}
	return @result;
}

my class DecodeBuffer {
	has buf8:D $!buffer is required;
	has Int:D $!offset = 0;
	submethod BUILD(:$!buffer) {
	}

	method decode-byte(--> Byte) {
		die InsufficientData.new('decode-byte') unless $!buffer.elems >= $!offset + 1;
		my $result = $!buffer.read-uint8($!offset);
		$!offset++;
		return $result;
	}

	method unpack-byte(*@selectors) {
		return unpack-flags(self.decode-byte, |@selectors);
	}

	method decode-short(--> Short) {
		die InsufficientData.new('decode-short') unless $!buffer.elems >= $!offset + 2;
		my $result = $!buffer.read-uint16($!offset, Endian::BigEndian);
		$!offset += 2;
		return $result;
	}

	method !decode-variable-length(Str $name) {
		my $len = self.decode-short;
		die InsufficientData.new("decode-$name") unless $!buffer.elems >= $!offset + $len;
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

our role Message[Type $type, Qos $qos = At-most-once] is export(:messages :decoder :types) {
	method message-type(--> Type) {
		return $type;
	}
	method header-byte(--> Byte) {
		return pack-flags(False, $qos, False, $type);
	}
	method decode-body(Message:U: DecodeBuffer $buffer, Int $flags --> Message:D) {
		...
	}
	method !encode-body(Message:D: EncodeBuffer --> Nil) {
		...
	}

	method encode(Message:D: --> Buf) {
		my $buffer = EncodeBuffer.new;
		self!encode-body($buffer);
		return buf8.new(self.header-byte) ~ $buffer.Buf;
	}
}

my role Message::Empty {
	method decode-body(Message:U: DecodeBuffer $, Int $ --> Message) {
		return self.new;
	}
	method !encode-body(Message:D: EncodeBuffer $ --> Nil) {
	}
}

my role Message::JustId {
	has Short:D $.packet-id is required;
	method decode-body(Message:U: DecodeBuffer $buffer, Int $ --> Message) {
		return self.new(:packet-id($buffer.decode-short));
	}
	method !encode-body(Message:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
	}
}

our class Message::Connect does Message[Type::Connect] is export(:messages :types) {
	has Str:D $.protocol-name = 'MQTT';
	has Byte:D $.protocol-version = 4;

	has Bool:D $.clean-start = True;

	has Short:D $.keep-alive-interval = 0;
	has Str:D $.client-identifier is required;

	class Will {
		has Str:D  $.topic is required;
		has Str:D  $.message is required;
		has Bool:D $.retain = False;
		has Qos:D  $.qos = At-most-once;
	}

	has Will $.will;

	has Str $.username;
	has Str $.password;

	method decode-body(DecodeBuffer $buffer, Int $  --> Message::Connect) {
		my $protocol-name = $buffer.decode-string;
		my $protocol-version = $buffer.decode-byte;

		my ($clean-start, $will-flag, $qos, $retain, $password-flag, $username-flag) = $buffer.unpack-byte(Skip, Bool, Bool, Qos, Bool, Bool, Bool);

		my %args = (:$protocol-name, :$protocol-version, :$clean-start);

		%args<keep-alive-interval> = $buffer.decode-short;
		%args<client-identifier> = $buffer.decode-string;

		if $will-flag {
			my $topic = $buffer.decode-string;
			my $message = $buffer.decode-string;
			%args<will> = Will.new(:$topic, :$message, :$qos, :$retain);
		}
		if $username-flag {
			%args<username> = $buffer.decode-string;
		}
		if $password-flag {
			%args<password> = $buffer.decode-string;
		}

		return self.new(|%args);
	}

	method !encode-body(Message::Connect:D: EncodeBuffer $buffer) {
		$buffer.encode-string($!protocol-name);
		$buffer.encode-byte($!protocol-version);
		$buffer.encode-byte(pack-flags(Skip, $!clean-start, ?$!will, $!will ?? $!will.qos !! At-most-once, $!will ?? $!will.retain !! False, $!password.defined, $!username.defined));
		$buffer.encode-short($!keep-alive-interval);
		$buffer.encode-string($!client-identifier);
		with $!will {
			$buffer.encode-string($!will.topic);
			$buffer.encode-string($!will.message);
		}
		$buffer.encode-string($!username) with $!username;
		$buffer.encode-string($!password) with $!password;
	}
}

class Message::ConnAck does Message[Type::ConnAck] is export(:messages :types) {
	has Bool:D $.session-acknowledge = False;
	has ConnectStatus:D $.return-code = Accepted;

	method decode-body(Message::ConnAck:U: DecodeBuffer $buffer, Int $) {
		my ($session-acknowledge) = $buffer.unpack-byte(Bool);
		my $return-code = ConnectStatus($buffer.decode-byte) orelse die InvalidValue.new('Invalid connect status');
		return self.new(:$session-acknowledge, :$return-code);
	}

	method !encode-body(Message::ConnAck:D: EncodeBuffer $buffer) {
		$buffer.encode-byte(pack-flags($.session-acknowledge));
		$buffer.encode-byte(+$.return-code);
	}
}

subset Topic is export(:subsets :types) of Str where /^ <-[\#\+]>* $ /;

class Message::Publish does Message[Type::Publish] is export(:messages :types) {
	has Qos:D $.qos = At-least-once;
	has Bool:D $.retain = False;
	has Bool:D $.dup = False;

	has Topic:D $.topic is required;
	has Short $.packet-id;
	has Blob:D $.message is required;

	submethod TWEAK(:$!qos = At-most-once) {
		die SemanticError.new('No packet-id on publish with qos') if $!qos > At-most-once && !$!packet-id.defined;
		die SemanticError.new('Can\'t duplicate qos-less message') if $!qos == At-most-once && $!dup;
	}
	method decode-body(Message:U: DecodeBuffer $buffer, Int $flags --> Message) {
		my ($retain, $qos, $dup) = unpack-flags($flags, Bool, Qos, Bool);

		my $topic = $buffer.decode-string;
		my $packet-id = $qos ?? $buffer.decode-short !! Short;
		my $message = $buffer.rest;

		return self.new(:$dup, :$qos, :$retain, :$topic, :$packet-id, :$message);
	}
	method header-byte(--> Byte) {
		return pack-flags($!retain, $!qos, $!dup, Type::Publish);
	}
	method !encode-body(Message:D: EncodeBuffer $buffer --> Nil) {
		$buffer.encode-string($!topic);
		$buffer.encode-short($!packet-id) if $!qos;
		$buffer.append-buffer($!message);
	}
}

our class Message::PubAck does Message[Type::PubAck] does Message::JustId is export(:messages :types) {
}

our class Message::PubRec does Message[Type::PubRec] does Message::JustId  is export(:messages :types) {
}

our class Message::PubRel does Message[Type::PubRel, At-least-once] does Message::JustId is export(:messages :types) {
}

our class Message::PubComp does Message[Type::PubComp] does Message::JustId is export(:messages :types) {
}

our class Filter is export(:types) {
	my sub to-matcher(Str $filter) {
		return !*.starts-with('$') if $filter eq '#';
		return *.starts-with('/') if $filter eq '/#';

		my $anchor = True;
		my @matchers;
		@matchers.push: /<!before '$'>/ if $filter ~~ / ^ \+ /;
		for $filter.comb(/ '/#' | '/' | <-[/]>+ /) {
			when '/#' { $anchor = False; last }
			when '+'  { @matchers.push: /<-[/]>*/ }
			default   { @matchers.push: $_ }
		}
		if all(@matchers) ~~ Str {
			my $string = @matchers.join;
			return $anchor ?? $string !! *.starts-with($string);
		}
		elsif $anchor {
			@matchers.push: / $ /;
		}
		@matchers.unshift: /^/;
		return @matchers.reduce({ /$^a$^b/ });
	}

	has Str:D $.topic is required;
	has Any:D $!matcher = to-matcher($!topic);

	method ACCEPTS(Str $string) {
		return $string ~~ $!matcher;
	}
}

our class Subscription is export(:types) {
	has Str:D $.topic = '#';
	has Qos:D $.qos = At-least-once;
}

our class Message::Subscribe does Message[Type::Subscribe, At-least-once] does Message::JustId is export(:messages :types) {
	has Subscription @.subscriptions is required;
	submethod TWEAK() {
		SemanticError.new('Subscribe without subscriptions is invalid') if not @!subscriptions;
	}
	method decode-body(Message:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @subscriptions;
		while $buffer.has-more {
			my $topic = $buffer.decode-string;
			my ($qos) = $buffer.unpack-byte(Qos);
			@subscriptions.push: Subscription.new(:$topic, :$qos);
		}
		return self.new(:$packet-id, :@subscriptions);
	}
	method !encode-body(Message:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!subscriptions -> $subscription {
			$buffer.encode-string($subscription.topic);
			$buffer.encode-byte(pack-flags($subscription.qos));
		}
	}
}

our class Message::SubAck does Message[Type::SubAck] does Message::JustId is export(:messages :types) {
	has Qos:D @.qos-levels;
	method decode-body(Message:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @qos-levels;
		while $buffer.has-more {
			@qos-levels.append: $buffer.unpack-byte(Qos);
		}
		return self.new(:$packet-id, :@qos-levels);
	}
	method !encode-body(Message:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!qos-levels -> $qos-level {
			$buffer.encode-byte(+$qos-level);
		}
	}
}

our class Message::Unsubscribe does Message[Type::Unsubscribe, At-least-once] does Message::JustId is export(:messages :types) {
	has Str @.subscriptions;

	method decode-body(Message:U: DecodeBuffer $buffer, Int $) {
		my $packet-id = $buffer.decode-short;
		my @subscriptions;
		while $buffer.has-more {
			@subscriptions.push: $buffer.decode-string;
		}
		return self.new(:$packet-id, :@subscriptions);
	}
	method !encode-body(Message:D: EncodeBuffer $buffer) {
		$buffer.encode-short($!packet-id);
		for @!subscriptions -> $subscription {
			$buffer.encode-string($subscription);
		}
	}
}

our class Message::UnsubAck does Message[Type::UnsubAck] does Message::JustId is export(:messages :types) {
}

our class Message::PingReq does Message[Type::PingReq] does Message::Empty is export(:messages :types) {
}

our class Message::PingResp does Message[Type::PingResp] does Message::Empty is export(:messages :types) {
}

our class Message::Disconnect does Message[Type::Disconnect] does Message::Empty is export(:messages :types) {
}

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

our class PacketBuffer is export(:decoder :types) {
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
			die InvalidValue.new('Invalid MQTT type') unless %class-for-type{$packet-type}:exists;
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

=begin pod

=head1 NAME

Protocol::MQTT - blah blah blah

=head1 SYNOPSIS

=begin code :lang<raku>

use Protocol::MQTT;

=end code

=head1 DESCRIPTION

Protocol::MQTT is ...

=head1 AUTHOR

Leon Timmermans <fawaka@gmail.com>

=head1 COPYRIGHT AND LICENSE

Copyright 2021 Leon Timmermans

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
