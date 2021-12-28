use v6.d;

unit class Net::MQTT;

use Protocol::MQTT::Client :state;
use Protocol::MQTT::Message;
use Protocol::MQTT::PacketBuffer;
use Protocol::MQTT::Qos :qos;

has IO::Socket::Async:U $!connection-class;
has IO::Socket::Async $!connection;
has Protocol::MQTT::Client $!client;
has Protocol::MQTT::PacketBuffer $!decoder = Protocol::MQTT::PacketBuffer.new;
has Protocol::MQTT::Dispatcher $!dispatcher = Protocol::MQTT::Dispatcher.new;
has Cancellation $!cue;
has Supplier  $!connected handles(:connected<Supply>)      = Supplier.new;
has Supplier $!disconnected handles(:disconnected<Supply>) = Supplier.new;
has Str $!server is required;
has Int $!port is required;
has Int:D $!connect-interval is required;
has Int:D $!reconnect-attempts is required;
has Int:D $!reconnect-tried is required;

submethod BUILD(
	Str:D :$!server!,
	Str:D :$client-identifier   = Protocol::MQTT::Client.generate-identifier,
	Bool  :$tls,
	Int:D :$!port               = $tls ?? 8883 !! 1883,
	Int:D :$keep-alive-interval = 60,
	Int:D :$resend-interval     = 5,
	Int:D :$!connect-interval   = $resend-interval,
	Int:D :$!reconnect-attempts = 3;
	Str   :$username,
	Str   :password($password-string),
	Protocol::MQTT::Message :$will;
	) {

	my Blob $password = $password-string.defined ?? $password-string.encode !! Nil;
	$!client = Protocol::MQTT::Client.new(:$client-identifier, :$keep-alive-interval, :$resend-interval, :$!connect-interval, :$username, :$password, :$will);
	$!client.incoming.tap: -> $message {
		$!dispatcher.dispatch($message);
	}
	$!client.disconnected.tap: -> $cause {
		self!disconnected;
	}

	$!connection-class = $tls ?? (require IO::Socket::Async::SSL) !! IO::Socket::Async;
	$!reconnect-tried = 0;
	self!connect;
}

method !connect(--> Nil) {
	$!connection-class.connect($!server, $!port).then: -> $connecting {
		if $connecting.status ~~ Kept {
			$!connection = $connecting.result;
			$!reconnect-tried = 0;

			my $connect-promise = $!client.connect;
			$connect-promise.then: -> $success {
				if $success.status ~~ Kept  {
					$!connected.emit($success.result);
				}
				else {
					self!disconnected;
				}
			}
			self!send-events(now);

			sub parser(buf8 $received) {
				my $now = now;
				$!decoder.add-data($received);
				while $!decoder.get-packet -> $packet {
					$!client.received-packet($packet, $now);
				}
				self!send-events($now);
			}
			sub done {
				$!cue.cancel with $!cue;
				$!disconnected.emit('Disconnected');
				if $!reconnect-tried < $!reconnect-attempts {
					self!connect;
					$!client.connect;
					self!send-events(now);
				}
			}
			$!connection.Supply(:bin).tap(&parser, :&done);
		}
		elsif self.$!reconnect-tried++ < $!reconnect-attempts {
			my $now = now;
			my $at = $now + $!connect-interval;
			$!cue = $*SCHEDULER.cue({ self!connect unless $!connection }, :$at)
		}
		else {
			$!connected.done;
			$!disconnected.emit('Couldn\'t connect');
		}
	}
}

method !disconnected(--> Nil) {
	my $connection = $!connection;
	$!connection = Nil;
	$connection.close;
}

method !send-events(Instant $now --> Nil) {
	$!cue.cancel with $!cue;
	for $!client.next-events($now) -> $packet {
		$!connection.write: $packet.encode;
	}
	my $at = $!client.next-expiration;

	my $callback = $!client.state === Unconnected ?? { self!connect } !! { self!send-events(now) }
	$!cue = $at ?? $*SCHEDULER.cue($callback, :$at) !! Nil;
}

method publish(Str:D $topic, Str:D $payload, Bool:D :$retain = False, Qos:D :$qos = At-most-once --> Promise:D) {
	my $now = now;
	my $result = $!client.publish($topic, $payload.encode, $qos, $retain, now);
	self!send-events($now);
	return $result;
}

method subscribe(Str:D $topic, Qos:D :$qos = At-most-once --> Supply:D) {
	my $now = now;
	my $result = $!dispatcher.add-filter($topic);
	$!client.subscribe($topic, $qos, $now);
	self!send-events($now);
	return $result;
}

method unsubscribe(Str:D $topic --> Promise:D) {
	my $now = now;
	$!dispatcher.remove-filter($topic);
	my $result = $!client.unsubscribe($topic, $now);
	self!send-events($now);
	return $result;
}

method disconnect(--> Nil) {
	$!reconnect-attempts = 0;
	$!client.disconnect;
	self!send-events(now);
	$!connection.close;
	$!connected.done;
}

=begin pod

=NAME Net::MQTT

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

C<Net::MQTT> is an implementation of a MQTT client.

=head1 METHODS

=head2 new(*%args)

This creates a new mqtt client. It takes the follow arguments, all optional unless indicated otherwise:

=begin item1
Str :$server

The name of the server to connect to. This argument is mandatory
=end item1

=begin item1
Str :$client-identifier

An unique identifier for this client/session. If none is given a random identifier will be generated.
=end item1

=begin item1
Bool :$tls

If true this enables tls. This requires `IO::Socket::Async::SSL`.
=end item1

=begin item1
Int :$port

This sets the port, it defaults to 1883 if tls is disabled, and 8883 if it is enabled
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
Str :$password

This sets the password, if any.
=end item1

=begin item1
Protocol::MQTT::Message :$will

This is the will of the connection. If the connection is lost without a formal disconnect, the server will send this message as if the client sent it.
=end item1

=head2 publish(Str $topic, Str $payload, Bool :$retain, Qos :$qos --> Promise)

This publishes C<$payload> to C<$topic>, with quality-of-service C<$qos> (defaulting to C<At-most-once>). If C<$retain> is true, it will be a retained message. The resulting Promise will be true once the confirmation of reception has been received (or immediately for C<At-most-once>).

=head2 subscribe(Str $topic, Qos :$qos --> Supply)

Subscribe to C<$topic>, asking for messages to be sent with quality-of-service C<$qos> (defaulting to C<At-most-once>).

=head2 unsubscribe($topic -> Promise)

Unsubscribe from C<$topic>.

=head2 disconnect(--> Nil)

This will send a disconnect message to the server, and then close the connection.

=head2 connected(--> Supply)

This supply will emit a value whenever a connection has been established. It will end when C<disconnect> is called, or if a connection breaks and can't be re-established.

=head2 disconnected(--> Supply)

This supply will emit a value whenever a connection has been lost.

=end pod

