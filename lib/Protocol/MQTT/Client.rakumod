use v6.d;

unit class Protocol::MQTT::Client:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Dispatcher;
use Protocol::MQTT::Message :message;
use Protocol::MQTT::Packet :packets;
use Protocol::MQTT::Qos :qos;

our enum ConnState is export(:state) <Disconnected Unconnected Connecting Connected Disconnecting>;

my class FollowUp {
	has Instant:D $.expiration    is required is rw;
	has Packet:D  $.packet        is required;
	has Promise:D $.promise       is required;
	has Int:D     $.order         is required;
}

has Str:D       $.client-identifier is required;
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
multi method received-packet(Packet::ConnAck:D $packet, Instant $now --> Nil) {
	if $packet.success {
		$!state = Connected;
		if $!keep-alive-interval {
			$!next-expiration = $now + $!keep-alive-interval;
		}
		if $packet.session-acknowledge {
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
		$!connect-promise.keep($packet.return-code);
		$!connect-promise = Nil;
	}
	else {
		$!disconnected.emit("Could not connect: " ~ $packet.return-code.subst('-', ' '));
	}
}
multi method received-packet(Packet::Publish:D (:$packet-id, :$topic, :$message, :$qos, :$retain, :$dup), Instant $ --> Nil) {
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
	my $update = Message.new(:$topic, :$message, :$qos, :$retain);
	$!incoming.emit($update);
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

method publish(Str:D $topic, Blob:D $message, Qos:D $qos, Bool:D $retain, Instant:D $now --> Promise:D) {
	given $qos {
		when At-most-once {
			@!queue.push: Packet::Publish.new(:$topic, :$message, :$retain, :$qos);
			return Promise.kept;
		}
		when At-least-once|Exactly-once {
			my $packet-id = self!next-id;
			my $resend = Packet::Publish.new(:$topic, :$message, :$retain, :$qos, :$packet-id, :dup);
			my $expiration = $now + $!resend-interval;
			@!queue.push: Packet::Publish.new(:$topic, :$message, :$retain, :$qos, :$packet-id);
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
