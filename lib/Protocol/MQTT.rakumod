use v6.d;

unit package Protocol::MQTT:ver<0.0.3>:auth<cpan:LEONT>;

our class Error is Exception {
}

our class Error::Semantic is Error {
	has Str $.message;
	method new($message) {
		return self.bless(:$message);
	}
}

our class Error::Decode is Error {
}

our class Error::InsufficientData is Error::Decode {
	has Str $.where;
	method new(Str $where) {
		return self.bless(:$where);
	}
	method message(--> Str) {
		return "$!where: insufficient data";
	}
}

our class Error::InvalidValue is Error::Decode {
	has Str:D $.message is required;
	method new(Str $message = 'Invalid value') {
		return self.bless(:$message);
	}
}

our enum Qos is export(:qos) (
	At-most-once  => 0x0,
	At-least-once => 0x1,
	Exactly-once  => 0x2,
);

our class Message {
	has Str:D   $.topic is required;
	has blob8:D $.payload is required;
	has Bool:D  $.retain = False;
	has Qos:D   $.qos = At-most-once;
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

our enum ConnectStatus (
	Accepted                              => 0,
	Refused-unacceptable-protocol-version => 1,
	Refused-identifier-rejected           => 2,
	Refused-server-unavailable            => 3,
	Refused-bad-user-name-or-password     => 4,
	Refused-not-authorized                => 5,
);

my role Skip[Int $count = 1] {
	method count() {
		return $count;
	}
}

my sub size-of-enum(Enumeration $enum) {
	state Int %size_of{Enumeration:U};
	return %size_of{$enum.WHAT} //= $enum.enums.keys.log2.ceiling;
}

my sub unpack-flags(Int $input, *@selectors) {
	my $index = 0;
	my @result;
	for @selectors {
		when Bool {
			@result.push(?($input +& (1 +< $index)));
			$index++;
		}
		when Enumeration {
			my $mask = (1 +< size-of-enum($_)) - 1;
			@result.push: $_(($input +> $index) +& $mask) // die Error::InvalidValue.new('Invalid ' ~ $_.^name);
			$index += 2;
		}
		when Skip {
			$index += .count;
		}
	}
	return @result;
}

subset Byte of Int where 0 .. 255;
subset Short of Int where 0 .. 65535;

subset Topic of Str where /^ <-[\#\+]>* $ /;

our class DecodeBuffer {
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

	method decode-blob(--> Buf) {
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

my sub pack-flags(*@values) {
	my $flag = 0;
	my $index = 0;
	for @values -> $value {
		when $value ~~ Bool {
			$flag +|= $value +< $index;
			$index++;
		}
		when $value ~~ Enumeration {
			$flag +|= $value +< $index;
			$index += size-of-enum($value);
		}
		when $value ~~ Skip {
			$index += $value.count;
		}
	}
	return $flag;
}

my class EncodeBuffer {
	has buf8 $!buffer = buf8.new;
	has Int $!offset = 0;

	method encode-byte(Byte $byte) {
		$!buffer.write-uint8($!offset, $byte);
		$!offset++;
	}

	method encode-byte-pack(*@values) {
		self.encode-byte(pack-flags(|@values));
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

our role Packet[Type $type, Qos $qos = At-most-once] {
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

our class Packet::Connect does Packet[Type::Connect] {
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
			my $payload = $buffer.decode-blob;
			%args<will> = Message.new(:$topic, :$payload, :$qos, :$retain);
		}
		if $username-flag {
			%args<username> = $buffer.decode-string;
		}
		if $password-flag {
			%args<password> = $buffer.decode-blob;
		}

		return self.new(|%args);
	}

	method !encode-body(Packet::Connect:D: EncodeBuffer $buffer) {
		$buffer.encode-string($!protocol-name);
		$buffer.encode-byte($!protocol-version);
		$buffer.encode-byte-pack(Skip, $!clean-start, ?$!will, $!will ?? $!will.qos !! At-most-once, $!will ?? $!will.retain !! False, $!password.defined, $!username.defined);
		$buffer.encode-short($!keep-alive-interval);
		$buffer.encode-string($!client-identifier);
		with $!will {
			$buffer.encode-string($!will.topic);
			$buffer.encode-blob($!will.payload);
		}
		$buffer.encode-string($!username) with $!username;
		$buffer.encode-blob($!password) with $!password;
	}
}

our class Packet::ConnAck does Packet[Type::ConnAck] {

	has Bool:D $.session-acknowledge = False;
	has ConnectStatus:D $.return-code = Accepted;

	method decode-body(Packet::ConnAck:U: DecodeBuffer $buffer, Int $) {
		my ($session-acknowledge) = $buffer.unpack-byte(Bool);
		my ($return-code) = $buffer.unpack-byte(ConnectStatus);
		return self.new(:$session-acknowledge, :$return-code);
	}

	method !encode-body(Packet::ConnAck:D: EncodeBuffer $buffer) {
		$buffer.encode-byte-pack($.session-acknowledge);
		$buffer.encode-byte-pack($.return-code);
	}
}

our class Packet::Publish does Packet[Type::Publish] {
	has Qos:D $.qos = At-least-once;
	has Bool:D $.retain = False;
	has Bool:D $.dup = False;

	has Str:D $.topic is required;
	has Short $.packet-id;
	has Blob:D $.payload is required;

	submethod TWEAK(:$!qos = At-most-once) {
		die Error::Semantic.new('') if $!topic !~~ Topic;
		die Error::Semantic.new('No packet-id on publish with qos') if $!qos > At-most-once && !$!packet-id.defined;
		die Error::Semantic.new('Can\'t duplicate qos-less message') if $!qos == At-most-once && $!dup;
	}
	method decode-body(Packet:U: DecodeBuffer $buffer, Int $flags --> Packet) {
		my ($retain, $qos, $dup) = unpack-flags($flags, Bool, Qos, Bool);

		my $topic = $buffer.decode-string;
		my $packet-id = $qos ?? $buffer.decode-short !! Short;
		my $payload = $buffer.rest;

		return self.new(:$dup, :$qos, :$retain, :$topic, :$packet-id, :$payload);
	}
	method header-byte(--> Byte) {
		return pack-flags($!retain, $!qos, $!dup, Type::Publish);
	}
	method !encode-body(Packet:D: EncodeBuffer $buffer --> Nil) {
		$buffer.encode-string($!topic);
		$buffer.encode-short($!packet-id) if $!qos;
		$buffer.append-buffer($!payload);
	}
}

our class Packet::PubAck does Packet[Type::PubAck] does Packet::JustId {
}

our class Packet::PubRec does Packet[Type::PubRec] does Packet::JustId {
}

our class Packet::PubRel does Packet[Type::PubRel, At-least-once] does Packet::JustId {
}

our class Packet::PubComp does Packet[Type::PubComp] does Packet::JustId {
}

our class Packet::Subscribe does Packet[Type::Subscribe, At-least-once] does Packet::JustId {
	class Subscription {
		has Str:D $.topic is required;
		has Qos:D $.qos is required;
	}
	has Subscription @.subscriptions is required;

	multi method new(Short:D :$packet-id!, Subscription :@subscription!) {
		self.bless(:$packet-id, :@subscription);
	}
	multi method new(Short:D :$packet-id!, Str:D :$topic!, Qos:D :$qos!) {
		my @subscriptions = Subscription.new(:$topic, :$qos);
		self.bless(:$packet-id, :@subscriptions);
	}
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
			$buffer.encode-byte-pack($subscription.qos);
		}
	}
}

our class Packet::SubAck does Packet[Type::SubAck] does Packet::JustId {
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

our class Packet::Unsubscribe does Packet[Type::Unsubscribe, At-least-once] does Packet::JustId {
	has Str @.subscriptions;

	multi method new(Short:D :$packet-id!, Str :@subscription!) {
		self.bless(:$packet-id, :@subscription);
	}
	multi method new(Short:D :$packet-id!, Str:D :$topic!) {
		self.bless(:$packet-id, :subscriptions[ $topic ]);
	}
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

our class Packet::UnsubAck does Packet[Type::UnsubAck] does Packet::JustId {
}

our class Packet::PingReq does Packet[Type::PingReq] does Packet::Empty {
}

our class Packet::PingResp does Packet[Type::PingResp] does Packet::Empty {
}

our class Packet::Disconnect does Packet[Type::Disconnect] does Packet::Empty {
}

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

our class PacketBuffer {
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

		my $offset = 1;
		my $remaining = decode-length($!buffer, $offset) orelse return Nil;

		if $!buffer.elems >= $offset + $remaining {
			my $packet-type = $!buffer.read-ubits(0, 4);
			my $flags = $!buffer.read-ubits(4, 4);
			my $buffer = $!buffer.subbuf($offset, $remaining);
			$!buffer.=subbuf($offset + $remaining);

			die Error::InvalidValue.new('Invalid MQTT type') unless %class-for-type{$packet-type}:exists;
			my $type = %class-for-type{$packet-type};

			my $decoder = DecodeBuffer.new(:$buffer);
			return $type.decode-body($decoder, $flags);
		}
		else {
			return Nil;
		}
	}
}

our class Filter {
	my sub to-matcher(Str $filter) {
		return !*.starts-with('$') if $filter eq '#';
		return *.starts-with('/') if $filter eq '/#';

		my $anchor = True;
		my @matchers;
		@matchers.push: /<!before '$'>/ if $filter ~~ / ^ \+ /;
		for $filter.comb(/ '/#' | '/' | <-[/]>+ /) {
			when '/#' {
				@matchers.push: / '/' | $ /;
				$anchor = False;
				last;
			}
			when '+'  { @matchers.push: /<-[/]>*/ }
			default   { @matchers.push: $_ }
		}
		if all(@matchers) ~~ Str {
			return @matchers.join;
		}
		elsif $anchor {
			@matchers.push: / $ /;
		}
		@matchers.unshift: /^/;
		return @matchers.reduce({ /$^a$^b/ });
	}

	has Str:D $.topic is required;
	has Any:D $!matcher handles<ACCEPTS> = to-matcher($!topic);
}

our class Dispatcher {
	my class Matcher {
		has Filter:D $.filter is required;
		has Supplier:D $.supplier is required;
	}

	has Matcher %!matchers;

	method add-filter(Str $topic --> Supply:D) {
		if %!matchers{$topic} -> $existing {
			return $existing.supplier.Supply;
		}
		else {
			my $supplier = Supplier.new;
			my $filter = Filter.new(:$topic);
			%!matchers{$topic} = Matcher.new(:$filter, :$supplier);
			return $supplier.Supply;
		}
	}

	method remove-filter(Str $topic --> Nil) {
		if %!matchers{$topic} -> $existing {
			%!matchers{$topic}.supplier.done;
			%!matchers{$topic}:delete;
		}
	}

	method dispatch(Message:D $message --> Nil) {
		for %!matchers.values -> (:$supplier, :$filter) {
			$supplier.emit($message) if $message.topic ~~ $filter;
		}
	}

	method clear(--> Nil) {
		for %!matchers.values -> $matcher {
			$matcher.supplier.done;
		}
		%!matchers = ();
	}
}

our enum ConnState is export(:state) <Disconnected Unconnected Connecting Connected Disconnecting>;

our class Client {
	my class FollowUp {
		has Instant:D $.expiration    is required is rw;
		has Packet:D  $.packet        is required;
		has Promise:D $.promise       is required;
		has Int:D     $.order         is required;
	}

	has Str:D       $.client-identifier                           = self.generate-identifier;
	has Int         $.keep-alive-interval                         = 60;
	has Int:D       $.resend-interval                             = 10;
	has Int:D       $.connect-interval                            = $!resend-interval;
	has Str         $.username;
	has Blob        $.password;
	has Message     $.will;
	has ConnState:D $.state                                       = Disconnected;
	has Supplier:D  $!disconnected handles(:disconnected<Supply>) = Supplier.new;
	has Bool        $!persistent-session                          = False;

	has Packet      @!queue;
	has Supplier    $!incoming handles(:incoming<Supply>)         = Supplier.new;
	has Instant     $!last-packet-received;
	has Instant     $!last-ping-sent;
	has Instant     $.next-expiration;
	has Int         $!packet-order                                = 0;
	has Promise     $!connect-promise;

	has FollowUp    %!follow-ups handles(:pending-acknowledgements<elems>);
	has Bool        %!blocked;
	has Qos         %!qos-for;

	submethod TWEAK(:$!keep-alive-interval) {
		die 'Oversized keep-alive interval' if $!keep-alive-interval !~~ Short;
	}

	method generate-identifier(:$prefix = 'raku-', :$length = 8, :@charset = 'a' .. 'z') {
		return $prefix ~ @charset.roll($length).join('');
	}

	method connect() {
		$!state = Unconnected;
		$!connect-promise.break if $!connect-promise && $!connect-promise.status ~~ Planned;
		$!connect-promise = Promise.new;
		return $!connect-promise;
	}

	method !add-follow-up(Int:D $packet-id, Packet:D $packet, Instant:D $expiration, Promise:D $promise = Promise.new --> Promise) {
		my $order = $!packet-order++;
		%!follow-ups{$packet-id} = FollowUp.new(:$packet, :$expiration, :$order, :$promise);
		return $promise;
	}

	method !confirm-follow-up(Int:D $packet-id) {
		if %!follow-ups{$packet-id} {
			%!follow-ups{$packet-id}.promise.keep;
			%!follow-ups{$packet-id}:delete;
		}
	}

	proto method received-packet(Packet:D, Instant $now) {
		$!last-packet-received = $now;
		{*}
	}
	multi method received-packet(Packet::ConnAck:D (:$return-code, :$session-acknowledge), Instant $now --> Nil) {
		if $return-code === Accepted {
			$!state = Connected;
			if $!keep-alive-interval {
				$!next-expiration = $now + $!keep-alive-interval;
			}
			if $session-acknowledge {
				for %!follow-ups.values -> $follow-up {
					@!queue.push: $follow-up.packet;
					$follow-up.expiration = $now + $!resend-interval;
				}
			}
			else {
				%!follow-ups = ();
				%!blocked = ();

				if %!qos-for {
					my @subscriptions = %!qos-for.kv.map: -> $topic, $qos { Packet::Subscribe::Subscription.new(:$topic, :$qos) }
					my $packet-id = self!next-id;
					my $subscribe = Packet::Subscribe.new(:$packet-id, :@subscriptions);
					@!queue.push: $subscribe;
					self!add-follow-up($packet-id, $subscribe, $now + $!resend-interval, $!connect-promise);
					$!connect-promise = Nil;
					return;
				}
			}
			$!connect-promise.keep($return-code);
			$!connect-promise = Nil;
		}
		else {
			$!disconnected.emit("Could not connect: " ~ $return-code.subst('-', ' '));
		}
	}
	multi method received-packet(Packet::Publish:D (:$packet-id, :$topic, :$payload, :$qos, :$retain, :$dup), Instant $ --> Nil) {
		given $qos {
			when At-least-once {
				@!queue.push: Packet::PubAck.new(:$packet-id);
			}
			when Exactly-once {
				@!queue.push: Packet::PubRec.new(:$packet-id);
				return if %!blocked{$packet-id};
				%!blocked{$packet-id} = True;
			}
		}
		my $message = Message.new(:$topic, :$payload, :$qos, :$retain);
		$!incoming.emit($message);
	}
	multi method received-packet(Packet::PubAck:D (:$packet-id), Instant $ --> Nil) {
		self!confirm-follow-up($packet-id);
	}
	multi method received-packet(Packet::PubRec:D (:$packet-id), Instant $now --> Nil) {
		if %!follow-ups{$packet-id} -> $publish {
			my $pubrel = Packet::PubRel.new(:$packet-id);
			@!queue.push: $pubrel;
			self!add-follow-up($packet-id, $pubrel, $now + $!resend-interval, $publish.promise);
		}
	}
	multi method received-packet(Packet::PubRel:D (:$packet-id), Instant $ --> Nil) {
		@!queue.push: Packet::PubComp.new(:$packet-id);
		%!blocked{$packet-id}:delete;
	}
	multi method received-packet(Packet::PubComp:D (:$packet-id), Instant $ --> Nil) {
		self!confirm-follow-up($packet-id);
	}
	multi method received-packet(Packet::SubAck:D (:$packet-id, :$qos-levels), Instant $ --> Nil) {
		self!confirm-follow-up($packet-id);
	}
	multi method received-packet(Packet::UnsubAck:D (:$packet-id), Instant $ --> Nil) {
		self!confirm-follow-up($packet-id);
	}
	multi method received-packet(Packet::PingResp:D, Instant $ --> Nil) {
	}

	method next-events(Instant:D $now --> List:D) {
		my @result;
		given $!state {
			when Unconnected {
				my $clean-session = !$!persistent-session;
				$!state = Connecting;
				$!last-ping-sent = $now;
				$!next-expiration = $now + $!connect-interval;
				@result.push: Packet::Connect.new(:$!client-identifier, :$!keep-alive-interval, :$clean-session, :$!username, :$!password, :$!will);
			}
			when Connecting {
				if $!next-expiration < $now {
					$!state = Unconnected;
					$!disconnected.emit('Connect timeout');
					$!next-expiration = $now + $!connect-interval;
				}
			}
			when Connected {
				if $!keep-alive-interval && $!last-packet-received + 2 * $!keep-alive-interval < $now {
					$!state = Disconnected;
					$!next-expiration = Nil;
					$!disconnected.emit('Timeout');
					succeed;
				}

				@result = @!queue.splice;

				my @expirations;
				for %!follow-ups.values.sort(*.order) -> $follow-up {
					if $follow-up.expiration < $now {
						@result.push: $follow-up.packet;
						$follow-up.expiration = $now + $!resend-interval;
					}
					@expirations.push: $follow-up.expiration;
				}

				if $!keep-alive-interval {
					if !@result && $!last-packet-received + $!keep-alive-interval < $now {
						@result.push: Packet::PingReq.new;
						$!last-ping-sent = $now;
					}
					@expirations.push: ($!last-packet-received max $!last-ping-sent) + $!keep-alive-interval;
				}

				$!next-expiration = @expirations ?? @expirations.min !! Nil;
			}
			when Disconnecting {
				$!state = Disconnected;
				$!next-expiration = Nil;
				%!follow-ups = ();
				%!blocked = ();
				@result.push: Packet::Disconnect.new;
			}
		}
		return @result;
	}

	method !next-id(--> Int:D) {
		loop {
			my $id = (1..65535).pick;
			return $id if not %!follow-ups{$id}:exists;
		}
	}

	method publish(Str:D $topic, Blob:D $payload, Qos:D $qos, Bool:D $retain, Instant:D $now --> Promise:D) {
		return Promise.broken('Invalid topic name') if $topic !~~ Topic;
		given $qos {
			when At-most-once {
				@!queue.push: Packet::Publish.new(:$topic, :$payload, :$retain, :$qos);
				return Promise.kept;
			}
			when At-least-once|Exactly-once {
				my $packet-id = self!next-id;
				my $resend = Packet::Publish.new(:$topic, :$payload, :$retain, :$qos, :$packet-id, :dup);
				my $expiration = $now + $!resend-interval;
				@!queue.push: Packet::Publish.new(:$topic, :$payload, :$retain, :$qos, :$packet-id);
				return self!add-follow-up($packet-id, $resend, $expiration);
			}
		}
	}

	multi method subscribe(Str:D $topic, Qos:D $qos, Instant:D $now --> Promise:D) {
		my $packet-id = self!next-id;
		my $packet = Packet::Subscribe.new(:$packet-id, :$topic, :$qos);
		my $expiration = $now + $!resend-interval;

		%!qos-for{$topic} = $qos;
		@!queue.push: $packet;
		return self!add-follow-up($packet-id, $packet, $expiration);
	}

	method unsubscribe(Str:D $topic, Instant:D $now --> Promise:D) {
		my $packet-id = self!next-id;
		my $packet = Packet::Unsubscribe.new(:$packet-id, :$topic);
		my $expiration = $now + $!resend-interval;

		@!queue.push: $packet;
		return self!add-follow-up($packet-id, $packet, $expiration);
	}

	method disconnect(--> Nil) {
		given $!state {
			when Unconnected|Connecting {
				$!connect-promise.break;
				$!state = Disconnected;
				$!next-expiration = Nil;
				%!follow-ups = ();
				%!blocked = ();
			}
			when Connected {
				$!state = Disconnecting;
			}
		}
	}
}



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
