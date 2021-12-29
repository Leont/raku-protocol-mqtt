use v6.d;

unit class Protocol::MQTT::Client:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Dispatcher;
use Protocol::MQTT::Message :message;
use Protocol::MQTT::Packet :packets;
use Protocol::MQTT::Qos :qos;
use Protocol::MQTT::Subsets;

our enum ConnState is export(:state) <Disconnected Unconnected Connecting Connected Disconnecting>;

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

=begin pod

=NAME Protocol::MQTT::Client

=head1 SYNOPSIS

=begin code
my $client = Net::MQTT.new(:server<localhost>);
react {
	whenever $client.subscribe('raku/updates/#') -> $update {
		say "{$update.topic}: {$update.payload.decode('utf8-c8')}";
	}
	whenever Supply.interval(20) {
		$client.publish('time/now', "Current time is {Datetime.now}", :retain);
	}
	whenever $client.connected {
		say "Connected";
		LAST { done }
	}
}
=end code

=head1 DESCRIPTION

C<Protocol::MQTT::Client> is a network and time independent implementation of a MQTT client.

=head1 METHODS

=head2 new(*%args)

This creates a new mqtt client. It takes the follow arguments, all are optional:

=begin item1
Str :$client-identifier

An unique identifier for this client/session. If none is given a random identifier will be generated.
=end item1

=begin item1
Int :$keep-alive-interval

This sets the keep-alive interval (in seconds). This defaults to C<60>.
=end item1

=begin item1
Int :$resend-interval

This sets the resend interval (in seconds). This defaults to C<5>.
=end item1

=begin item1
Int :$connect-interval

This sets the connect interval. It defaults to the resend interval.
=end item1

=begin item1
Int :$reconnect-attempts

This sets how many times it will try to (re)connect before giving up. This defaults to C<3>.
=end item1

=begin item1
Str :$username

This sets the username, if any.
=end item1

=begin item1
Blob :$password

This sets the password, if any.
=end item1

=begin item1
Protocol::MQTT::Message :$will

This is the will of the connection. If the connection is lost without a formal disconnect, the server will send this message as if the client sent it.
=end item1

=head2 next-events(Instant $now -> List:D)

This returns a list of packets to be sent over the connection. This B<must> be called after any method that changes (C<connect>, C<disconnect>, C<publish>, C<subscribe>, C<unsubscribe>, C<received-packet>), or when C<next-expiration> times out.

=head2 connect()

This changes the state to connecting.

=head2 disconnect(--> Nil)

This marks the connection for disconnecting.

=head2 publish(Str:D $topic, Blob:D $payload, Qos:D $quality-of-service, Bool:D $retainedness, Instant:D $now --> Promise:D)

This publishes C<$payload> to C<$topic>, with the specified C<$quality-of-service> and C<$retainedness>. The resulting promise will succeed when the QOS requirements are met (or fail if the connection is lost before that happens).

=head2 subscribe(Str:D $topic, Qos:D $qos, Instant:D $now --> Promise:D)

This subscribes to a specific topic with messages being sent using the specified quality-of-service. The messages will be posted on the C<incoming> C<Supply>.

=head2 unsubscribe(Str:D $topic, Instant:D $now --> Promise:D)

This unsubscribes from a C<$topic>.

=head2 received-packet(Protocol::MQTT::Packet $packet, Instant $now)

This should be called with every incoming C<Packet>. Turning a data-stream into packets is done using L<Protocol::MQTT::PacketBuffer|Protocol::MQTT::PacketBuffer>.

=head2 next-expiration(--> Instant:D)

This returns the next C<Instant> when current outstanding message expire and hence C<next-events> should be called again, if any.

=head2 incoming(--> Supply:D)

This returns the supply of Protocol::MQTT::Message objects that have been received from the server.

=head2 disconnected(--> Promise:D)

This promise will succeed whenever the connection is lost (connecting failed, or we failed to receive keep-alives for too long).

=end pod
